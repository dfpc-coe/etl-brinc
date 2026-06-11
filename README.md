<h1 align='center'>ETL-Brinc</h1>

<p align='center'>Pull Brinc UAS drone telemetry into the TAK System</p>

## Development

DFPC provided Lambda ETLs are currently all written in [NodeJS](https://nodejs.org/en) through the use of a AWS Lambda optimized
Docker container. Documentation for the Dockerfile can be found in the [AWS Help Center](https://docs.aws.amazon.com/lambda/latest/dg/images-create.html)

```sh
npm install
```

Add a .env file in the root directory that gives the ETL script the necessary variables to communicate with a local ETL server.
When the ETL is deployed the `ETL_API` and `ETL_LAYER` variables will be provided by the Lambda Environment

```json
{
    "ETL_API": "http://localhost:5001",
    "ETL_LAYER": "19",
    "BRINC_OPAQUE_ID": "your-opaque-id",
    "BRINC_CLIENT_ID": "your-client-id",
    "BRINC_CLIENT_SECRET": "your-client-secret"
}
```

To run the task, ensure the local [CloudTAK](https://github.com/dfpc-coe/CloudTAK/) server is running and then run with typescript runtime
or build to JS and run natively with node

```
ts-node task.ts
```

```
npm run build
cp .env dist/
node dist/task.js
```

### Configuration

The ETL requires the following environment variables to authenticate with the Brinc LiveOps API:

- `BRINC_OPAQUE_ID`: Your LiveOps Organization's opaque ID
- `BRINC_CLIENT_ID`: App Client ID with `drone-telemetry.read` scope
- `BRINC_CLIENT_SECRET`: App Client secret

These credentials can be obtained from your organization administrator or by logging into LiveOps and visiting the Developer Portal.

### Deployment

Deployment into the CloudTAK environment for configuration is done via automatic releases to the DFPC AWS environment.

Github actions will build and push docker releases on every version tag which can then be automatically configured via the 
CloudTAK API.

Non-DFPC users will need to setup their own docker => ECS build system via something like Github Actions or AWS Codebuild.

