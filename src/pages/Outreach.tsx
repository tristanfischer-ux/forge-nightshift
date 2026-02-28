import { useEffect, useState } from "react";
import { Mail, Send, Eye } from "lucide-react";
import { getEmails, updateEmailStatus } from "../lib/tauri";

const STATUS_COLORS: Record<string, string> = {
  draft: "bg-gray-700/50 text-gray-300",
  approved: "bg-blue-900/50 text-blue-300",
  sending: "bg-yellow-900/50 text-yellow-300",
  sent: "bg-green-900/50 text-green-300",
  opened: "bg-purple-900/50 text-purple-300",
  replied: "bg-emerald-900/50 text-emerald-300",
  bounced: "bg-red-900/50 text-red-300",
  failed: "bg-red-900/50 text-red-300",
};

export default function Outreach() {
  const [emails, setEmails] = useState<Record<string, unknown>[]>([]);
  const [selectedEmail, setSelectedEmail] = useState<Record<
    string,
    unknown
  > | null>(null);

  useEffect(() => {
    loadEmails();
  }, []);

  async function loadEmails() {
    try {
      const data = await getEmails(undefined, 100);
      setEmails(data);
    } catch {
      // DB may not be ready
    }
  }

  async function handleApproveEmail(id: string) {
    await updateEmailStatus(id, "approved");
    loadEmails();
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Outreach</h1>
        <p className="text-sm text-forge-400 mt-1">
          Email drafts, approvals, and send tracking
        </p>
      </div>

      <div className="flex gap-4">
        {/* Email list */}
        <div className="flex-1 bg-forge-900/50 rounded-xl border border-forge-800/50">
          <div className="p-4 border-b border-forge-800/50">
            <h2 className="text-sm font-semibold">
              Email Queue ({emails.length})
            </h2>
          </div>

          <div className="divide-y divide-forge-800/30 max-h-[calc(100vh-220px)] overflow-y-auto">
            {emails.length === 0 ? (
              <div className="p-8 text-center text-forge-500 text-sm">
                No emails yet. Run the outreach pipeline stage to generate
                emails.
              </div>
            ) : (
              emails.map((email) => (
                <div
                  key={String(email.id)}
                  className={`flex items-center gap-3 px-4 py-3 cursor-pointer transition-colors ${
                    selectedEmail?.id === email.id
                      ? "bg-forge-700/30"
                      : "hover:bg-forge-800/20"
                  }`}
                  onClick={() => setSelectedEmail(email)}
                >
                  <Mail className="w-4 h-4 text-forge-500 shrink-0" />
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium truncate">
                      {String(email.company_name || email.to_email || "")}
                    </p>
                    <p className="text-xs text-forge-400 truncate">
                      {String(email.subject || "")}
                    </p>
                  </div>
                  <span
                    className={`px-2 py-0.5 rounded-full text-xs ${STATUS_COLORS[String(email.status)] || STATUS_COLORS.draft}`}
                  >
                    {String(email.status || "")}
                  </span>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Email preview */}
        {selectedEmail && (
          <div className="w-[480px] bg-forge-900/50 rounded-xl border border-forge-800/50 p-4 space-y-4">
            <div>
              <h3 className="text-sm font-semibold">
                {String(selectedEmail.subject || "")}
              </h3>
              <p className="text-xs text-forge-400 mt-1">
                To: {String(selectedEmail.to_email || "")}
              </p>
            </div>

            <div className="bg-forge-950/50 rounded-lg p-4 max-h-80 overflow-y-auto">
              <div
                className="text-sm text-forge-200 prose prose-invert prose-sm"
                dangerouslySetInnerHTML={{
                  __html: String(selectedEmail.body || ""),
                }}
              />
            </div>

            {selectedEmail.status === "draft" && (
              <div className="flex gap-2">
                <button
                  onClick={() =>
                    handleApproveEmail(String(selectedEmail.id))
                  }
                  className="flex-1 flex items-center justify-center gap-2 px-4 py-2 bg-green-700 hover:bg-green-800 rounded-lg text-sm font-medium transition-colors"
                >
                  <Send className="w-4 h-4" />
                  Approve & Send
                </button>
              </div>
            )}

            {selectedEmail.sent_at ? (
              <div className="flex items-center gap-2 text-xs text-forge-500">
                <Eye className="w-3 h-3" />
                {"Sent: " + String(selectedEmail.sent_at)}
                {selectedEmail.opened_at
                  ? " | Opened: " + String(selectedEmail.opened_at)
                  : null}
              </div>
            ) : null}
          </div>
        )}
      </div>
    </div>
  );
}
