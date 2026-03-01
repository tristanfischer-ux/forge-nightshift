import { check } from "@tauri-apps/plugin-updater";
import { relaunch } from "@tauri-apps/plugin-process";

export interface UpdateInfo {
  version: string;
  body?: string;
}

export async function checkForUpdate(): Promise<UpdateInfo | null> {
  try {
    const update = await check();
    if (update) {
      return {
        version: update.version,
        body: update.body ?? undefined,
      };
    }
    return null;
  } catch {
    // latest.json 404s until first release — expected
    return null;
  }
}

export async function installUpdate(
  onProgress?: (percent: number) => void
): Promise<void> {
  const update = await check();
  if (!update) return;

  let totalLength = 0;
  let downloaded = 0;

  await update.downloadAndInstall((event) => {
    if (event.event === "Started" && event.data.contentLength) {
      totalLength = event.data.contentLength;
    } else if (event.event === "Progress") {
      downloaded += event.data.chunkLength;
      if (totalLength > 0 && onProgress) {
        onProgress(Math.round((downloaded / totalLength) * 100));
      }
    }
  });

  await relaunch();
}
