import type { Static, TSchema } from '@sinclair/typebox';
import { Type } from '@sinclair/typebox';
import type { Event } from '@tak-ps/etl';
import { Feature } from '@tak-ps/node-cot'
import ETL, { SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';

const InputSchema = Type.Object({
    'BRINC_OPAQUE_ID': Type.String({
        description: 'Brinc LiveOps Organization Opaque ID'
    }),
    'BRINC_CLIENT_ID': Type.String({
        description: 'Brinc LiveOps App Client ID'
    }),
    'BRINC_CLIENT_SECRET': Type.String({
        description: 'Brinc LiveOps App Client Secret'
    }),
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
});

const OutputSchema = Type.Object({
    deviceId: Type.String(),
    flightUUID: Type.Optional(Type.String()),
    flightTime: Type.Optional(Type.Number()),
    altAgl: Type.Optional(Type.Number()),
    altMsl: Type.Optional(Type.Number()),
    horizontalSpeed: Type.Optional(Type.Number()),
    heading: Type.Optional(Type.Number()),
    verticalSpeed: Type.Optional(Type.Number()),
    armed: Type.Optional(Type.Boolean()),
    batteryPercent: Type.Optional(Type.Number()),
    charging: Type.Optional(Type.Boolean()),
    softwareVersion: Type.Optional(Type.String()),
    online: Type.Optional(Type.Boolean())
})

export default class Task extends ETL {
    static name = 'etl-brinc'
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return InputSchema;
            } else {
                return OutputSchema;
            }
        } else {
            return Type.Object({});
        }
    }

    async control(): Promise<void> {
        const env = await this.env(InputSchema);

        if (!env.BRINC_OPAQUE_ID) throw new Error('No BRINC_OPAQUE_ID Provided');
        if (!env.BRINC_CLIENT_ID) throw new Error('No BRINC_CLIENT_ID Provided');
        if (!env.BRINC_CLIENT_SECRET) throw new Error('No BRINC_CLIENT_SECRET Provided');

        console.log('ok - requesting access token from Brinc LiveOps');

        const tokenUrl = new URL(`/v1/${env.BRINC_OPAQUE_ID}/token`, 'https://api.brincdrones.com');
        const tokenBody = new URLSearchParams({
            client_id: env.BRINC_CLIENT_ID,
            client_secret: env.BRINC_CLIENT_SECRET,
            scope: 'drone-telemetry.read',
            grant_type: 'client_credentials'
        });

        const tokenRes = await fetch(tokenUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: tokenBody
        });

        if (!tokenRes.ok) {
            throw new Error(`Failed to get access token: ${tokenRes.status} ${tokenRes.statusText}`);
        }

        const tokenData = await tokenRes.json() as { access_token: string };
        const accessToken = tokenData.access_token;

        console.log('ok - access token obtained');

        console.log('ok - listing drones from Brinc LiveOps');

        const listUrl = new URL(`/v1/${env.BRINC_OPAQUE_ID}/devices/drones`, 'https://api.brincdrones.com');
        listUrl.searchParams.append('pageSize', '200');

        const deviceIds: string[] = [];
        let pageToken: string | undefined;

        do {
            if (pageToken) {
                listUrl.searchParams.set('pageToken', pageToken);
            }

            const listRes = await fetch(listUrl, {
                headers: { 'Authorization': `Bearer ${accessToken}` }
            });

            if (!listRes.ok) {
                throw new Error(`Failed to list drones: ${listRes.status} ${listRes.statusText}`);
            }

            const listData = await listRes.json() as { items: Array<{ deviceId: string }>; nextPageToken?: string };
            
            for (const drone of listData.items) {
                deviceIds.push(drone.deviceId);
            }

            pageToken = listData.nextPageToken;
        } while (pageToken);

        console.log(`ok - found ${deviceIds.length} drones`);

        if (deviceIds.length === 0) {
            const fc: Static<typeof Feature.InputFeatureCollection> = {
                type: 'FeatureCollection',
                features: []
            };
            await this.submit(fc);
            return;
        }

        console.log('ok - requesting telemetry for drones');

        const telemetryUrl = new URL(`/v1/${env.BRINC_OPAQUE_ID}/devices/drones/telemetry`, 'https://api.brincdrones.com');
        for (const deviceId of deviceIds) {
            telemetryUrl.searchParams.append('deviceIds', deviceId);
        }

        const telemetryRes = await fetch(telemetryUrl, {
            headers: { 'Authorization': `Bearer ${accessToken}` }
        });

        if (!telemetryRes.ok) {
            throw new Error(`Failed to get telemetry: ${telemetryRes.status} ${telemetryRes.statusText}`);
        }

        const telemetryData = await telemetryRes.json() as {
            items: Array<{
                deviceId: string;
                flightUUID?: string;
                flightTime?: number;
                lat?: number;
                long?: number;
                altAgl?: number;
                altMsl?: number;
                horizontalSpeed?: number;
                heading?: number;
                verticalSpeed?: number;
                armed?: boolean;
                batteryPercent?: number;
                charging?: boolean;
                softwareVersion?: string;
                online?: boolean;
            }>;
            errors: Array<{ deviceId: string; code: string; message: string }>;
        };

        console.log(`ok - received telemetry for ${telemetryData.items.length} drones`);

        if (env.DEBUG && telemetryData.errors.length > 0) {
            console.log(`ok - ${telemetryData.errors.length} drones had errors:`, telemetryData.errors);
        }

        const features: Static<typeof Feature.InputFeature>[] = [];

        for (const drone of telemetryData.items) {
            if (drone.lat === undefined || drone.long === undefined) {
                if (env.DEBUG) {
                    console.log(`skipping drone ${drone.deviceId} - no location data`);
                }
                continue;
            }

            const id = `brinc-${drone.deviceId}`;
            const feat: Static<typeof Feature.InputFeature> = {
                id,
                type: 'Feature',
                properties: {
                    course: drone.heading ?? 0,
                    speed: (drone.horizontalSpeed ?? 0) * 0.44704, // mph => m/s
                    callsign: drone.deviceId,
                    time: new Date().toISOString(),
                    start: new Date().toISOString(),
                    remarks: `Brinc Drone ${drone.deviceId}${drone.online ? ' (Online)' : ' (Offline)'}`,
                    metadata: {
                        deviceId: drone.deviceId,
                        flightUUID: drone.flightUUID,
                        flightTime: drone.flightTime,
                        altAgl: drone.altAgl,
                        altMsl: drone.altMsl,
                        horizontalSpeed: drone.horizontalSpeed,
                        heading: drone.heading,
                        verticalSpeed: drone.verticalSpeed,
                        armed: drone.armed,
                        batteryPercent: drone.batteryPercent,
                        charging: drone.charging,
                        softwareVersion: drone.softwareVersion,
                        online: drone.online
                    }
                },
                geometry: {
                    type: 'Point',
                    coordinates: [drone.long, drone.lat, drone.altMsl ? drone.altMsl * 0.3048 : 0]
                }
            };

            features.push(feat);
        }

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: features
        };

        await this.submit(fc);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

