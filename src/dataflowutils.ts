import {
    TemplatesServiceClient,
    protos,
    JobsV1Beta3Client,
} from "@google-cloud/dataflow";
import { Storage, DownloadResponse } from "@google-cloud/storage";
// @ts-ignore
import yaml from "js-yaml";
import { ClientOptions } from "google-gax";
import { getLatestVersion } from "./versionutils";
type Paths = {
    properties: string;
    template: string;
    version: string;
};
type RuntimeEnvironment = protos.google.dataflow.v1beta3.RuntimeEnvironment;
type IJob = protos.google.dataflow.v1beta3.IJob;
type ICreateJobFromTemplateRequest =
    protos.google.dataflow.v1beta3.ICreateJobFromTemplateRequest;
type Properties = {
    environment: RuntimeEnvironment;
    parameters?: Record<string, string> | null;
};
type Request = Properties & {
    gcsPath: string;
    jobName: string;
    projectId: string;
};
export type DataflowUtilOptions = ClientOptions & {
    bucketName: string;
    artifactId: string;
    dataflowName: string;
};

let client: TemplatesServiceClient;
let jobsClient: JobsV1Beta3Client;
let storage: Storage;
export class DataflowUtils {
    readonly #bucketName: string;
    readonly #artifactId: string;
    readonly #dataflowName: string;
    constructor(options: DataflowUtilOptions) {
        const { bucketName, artifactId, dataflowName } = options;
        this.#bucketName = bucketName;
        this.#artifactId = artifactId;
        this.#dataflowName = dataflowName;
        client = new TemplatesServiceClient(options);
        jobsClient = new JobsV1Beta3Client(options);
        storage = new Storage();
    }

    deploy(
        version?: string
    ): Promise<
        [IJob, ICreateJobFromTemplateRequest | undefined, {} | undefined]
    > {
        return new Promise(async (resolve, reject) => {
            try {
                const request = await this.#getRequest(version);
                resolve(client.createJobFromTemplate(request));
            } catch (err) {
                reject(err);
            }
        });
    }

    #getRequest(versionOverride?: string | undefined): Promise<Request> {
        return new Promise(async (resolve, reject) => {
            try {
                const {
                    properties,
                    template: gcsPath,
                    version,
                } = await this.#getPaths(versionOverride);
                const projectId = await client.getProjectId();
                const jobName = `${this.#artifactId}-${version}-a`;
                resolve({
                    ...this.#parse(properties, await this.#getFile(properties)),
                    gcsPath,
                    projectId,
                    jobName,
                });
            } catch (err) {
                reject(err);
            }
        });
    }

    #getPaths(versionOverride?: string | undefined): Promise<Paths> {
        return new Promise(async (resolve, reject) => {
            try {
                const artifactId = this.#artifactId;
                const bucketName = this.#bucketName;
                const dataflowName = this.#dataflowName;
                const options = {
                    artifactId,
                    bucketName,
                    projectId: await client.getProjectId(),
                };
                const version =
                    versionOverride ?? (await getLatestVersion(options));
                resolve({
                    properties: `gs://${bucketName}/templates/${artifactId}/${version}/${dataflowName}/properties.yaml`,
                    template: `gs://${bucketName}/templates/${artifactId}/${version}/${dataflowName}/${artifactId}-${version}`,
                    version,
                });
            } catch (err) {
                reject(err);
            }
        });
    }

    #getFile(filename: string): Promise<DownloadResponse> {
        return new Promise(async (resolve, reject) => {
            try {
                if (!this.#getExtension(filename)) {
                    reject("Invalid file extension");
                    return;
                }
                const filePaths = filename.split("gs://")[1]?.split("/");
                if (!filePaths) {
                    reject("Invalid GS filepath");
                    return;
                }
                const bucket = filePaths[0];
                const file = filePaths.splice(1).join("/");
                resolve(storage.bucket(bucket).file(file).download());
            } catch (err) {
                reject(err);
            }
        });
    }

    #parse(filename: string, file: DownloadResponse): Properties {
        let properties: Properties;
        switch (this.#getExtension(filename)) {
            case "yaml":
                properties = yaml.load(file.toString()) as Properties;
                break;
            case "yml":
                properties = yaml.load(file.toString()) as Properties;
                break;
            case "json":
                properties = JSON.parse(file.toString());
                break;
            default:
                throw Error(
                    "Failed to parse file. Invalid extension. Must be yaml or json."
                );
        }
        return properties;
    }

    #getExtension(filename: string) {
        return (
            filename.substring(
                filename.lastIndexOf(".") + 1,
                filename.length
            ) || filename
        );
    }
}

export async function getStatus(
    projectId: string,
    jobId: string
): Promise<
    | protos.google.dataflow.v1beta3.JobState
    | "JOB_STATE_UNKNOWN"
    | "JOB_STATE_STOPPED"
    | "JOB_STATE_RUNNING"
    | "JOB_STATE_DONE"
    | "JOB_STATE_FAILED"
    | "JOB_STATE_CANCELLED"
    | "JOB_STATE_UPDATED"
    | "JOB_STATE_DRAINING"
    | "JOB_STATE_DRAINED"
    | "JOB_STATE_PENDING"
    | "JOB_STATE_CANCELLING"
    | "JOB_STATE_QUEUED"
    | "JOB_STATE_RESOURCE_CLEANING_UP"
    | null
    | undefined
> {
    if (!jobsClient) {
        jobsClient = new JobsV1Beta3Client();
    }
    const [jobs] = await jobsClient.listJobs({ projectId });
    return jobs.filter(({ id }) => id === jobId)[0].currentState;
}
