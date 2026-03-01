import { useEffect, useState } from "react";
import { Download, X } from "lucide-react";
import { checkForUpdate, installUpdate, UpdateInfo } from "../lib/updater";

export default function UpdateBanner() {
  const [update, setUpdate] = useState<UpdateInfo | null>(null);
  const [dismissed, setDismissed] = useState(false);
  const [installing, setInstalling] = useState(false);
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    const timer = setTimeout(async () => {
      const info = await checkForUpdate();
      if (info) setUpdate(info);
    }, 3000);
    return () => clearTimeout(timer);
  }, []);

  if (!update || dismissed) return null;

  const handleInstall = async () => {
    setInstalling(true);
    try {
      await installUpdate(setProgress);
    } catch {
      setInstalling(false);
    }
  };

  return (
    <div className="flex items-center justify-between bg-blue-50 border border-blue-200 rounded-lg px-4 py-2 mb-4">
      <div className="flex items-center gap-2 text-sm text-blue-800">
        <Download size={16} />
        <span>
          Version <strong>{update.version}</strong> is available.
        </span>
      </div>
      <div className="flex items-center gap-2">
        {installing ? (
          <span className="text-sm text-blue-600">
            {progress > 0 ? `Downloading ${progress}%...` : "Preparing..."}
          </span>
        ) : (
          <button
            onClick={handleInstall}
            className="text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 px-3 py-1 rounded-md transition-colors"
          >
            Update & Restart
          </button>
        )}
        {!installing && (
          <button
            onClick={() => setDismissed(true)}
            className="text-blue-400 hover:text-blue-600 transition-colors"
          >
            <X size={16} />
          </button>
        )}
      </div>
    </div>
  );
}
