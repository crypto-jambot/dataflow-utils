// @ts-ignore
import sort from "version-sort";
import { Storage, File } from "@google-cloud/storage";
let storage: Storage;
type InitialOptions = {
    projectId: string;
    bucketName: string;
    artifactId: string;
};
type Options = InitialOptions & {
    folderIndex: number;
};
async function getFiles(options: Options): Promise<File[]> {
    const { projectId, bucketName, artifactId } = options;
    const prefix = `templates/${artifactId}`;
    if (!storage) {
        storage = new Storage({ projectId });
    }
    const [files] = await storage.bucket(bucketName).getFiles({ prefix });
    return files;
}

function getVersions(options: Options): Promise<string[]> {
    const { folderIndex } = options;
    return new Promise(async (resolve, reject) => {
        try {
            const files = await getFiles(options);
            const versions = files.map((file) => {
                const { name } = file;
                return name.split("/")[folderIndex];
            });
            const dedup = [...new Set(versions)];
            resolve(dedup.length < 2 ? dedup : sort(dedup));
        } catch (err) {
            reject(err);
        }
    });
}

export function getLatestVersion(options: InitialOptions): Promise<string> {
    return new Promise(async (resolve, reject) => {
        try {
            const versions = await getVersions({ ...options, folderIndex: 2 });
            if (!versions || versions.length < 1) {
                reject("Failed to fetch latest version in folder structure");
                return;
            }
            resolve(versions.pop() as string);
        } catch (err) {
            reject(err);
        }
    });
}
