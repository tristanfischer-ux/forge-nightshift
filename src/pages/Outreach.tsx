import { useEffect, useState } from "react";
import { Mail, Send, Eye, RefreshCw, Loader2 } from "lucide-react";
import DOMPurify from "dompurify";
import { getEmails, updateEmailStatus, refreshEmailStatuses } from "../lib/tauri";
import { useError } from "../contexts/ErrorContext";

const STATUS_COLORS: Record<string, string> = {
  draft: "bg-gray-100 text-gray-600",
  approved: "bg-blue-100 text-blue-700",
  sending: "bg-yellow-100 text-yellow-700",
  sent: "bg-green-100 text-green-700",
  opened: "bg-purple-100 text-purple-700",
  replied: "bg-emerald-100 text-emerald-700",
  bounced: "bg-red-100 text-red-700",
  failed: "bg-red-100 text-red-700",
};

export default function Outreach() {
  const { showError } = useError();
  const [emails, setEmails] = useState<Record<string, unknown>[]>([]);
  const [selectedEmail, setSelectedEmail] = useState<Record<
    string,
    unknown
  > | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshCount, setRefreshCount] = useState<number | null>(null);

  useEffect(() => {
    loadEmails();
  }, []);

  async function loadEmails() {
    setLoading(true);
    try {
      const data = await getEmails(undefined, 100);
      setEmails(data);
    } catch (e) {
      showError(`Failed to load emails: ${e}`);
    }
    setLoading(false);
  }

  async function handleApproveEmail(id: string) {
    try {
      await updateEmailStatus(id, "approved");
      await loadEmails();
    } catch (e) {
      showError(`Failed to approve email: ${e}`);
    }
  }

  async function handleRefreshStatuses() {
    setRefreshing(true);
    setRefreshCount(null);
    try {
      const count = await refreshEmailStatuses();
      setRefreshCount(count);
      await loadEmails();
    } catch (e) {
      showError(`Failed to refresh statuses: ${e}`);
      setRefreshCount(-1);
    }
    setRefreshing(false);
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Outreach</h1>
          <p className="text-sm text-gray-500 mt-1">
            Email drafts, approvals, and send tracking
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={handleRefreshStatuses}
            disabled={refreshing}
            className="flex items-center gap-2 px-3 py-1.5 bg-gray-100 hover:bg-gray-200 disabled:opacity-50 rounded-lg text-xs text-gray-700 transition-colors"
          >
            {refreshing ? (
              <Loader2 className="w-3 h-3 animate-spin" />
            ) : (
              <RefreshCw className="w-3 h-3" />
            )}
            Refresh Status
          </button>
          {refreshCount !== null && refreshCount >= 0 && (
            <span className="text-xs text-green-600">
              {refreshCount} updated
            </span>
          )}
          {refreshCount === -1 && (
            <span className="text-xs text-red-600">Failed</span>
          )}
        </div>
      </div>

      <div className="flex gap-4">
        {/* Email list */}
        <div className="flex-1 bg-white rounded-xl border border-gray-200 shadow-sm">
          <div className="p-4 border-b border-gray-200">
            <h2 className="text-sm font-semibold text-gray-900">
              Email Queue ({emails.length})
            </h2>
          </div>

          <div className="divide-y divide-gray-100 max-h-[calc(100vh-220px)] overflow-y-auto">
            {loading ? (
              <div className="flex items-center justify-center p-8">
                <Loader2 className="w-5 h-5 text-gray-400 animate-spin" />
              </div>
            ) : emails.length === 0 ? (
              <div className="p-8 text-center text-gray-400 text-sm">
                No emails yet. Run the outreach pipeline stage to generate
                emails.
              </div>
            ) : (
              emails.map((email) => (
                <div
                  key={String(email.id)}
                  className={`flex items-center gap-3 px-4 py-3 cursor-pointer transition-colors ${
                    selectedEmail?.id === email.id
                      ? "bg-blue-50"
                      : "hover:bg-gray-50"
                  }`}
                  onClick={() => setSelectedEmail(email)}
                >
                  <Mail className="w-4 h-4 text-gray-400 shrink-0" />
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium text-gray-900 truncate">
                      {String(email.company_name || email.to_email || "")}
                    </p>
                    <p className="text-xs text-gray-500 truncate">
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
          <div className="w-[480px] bg-white rounded-xl border border-gray-200 p-4 space-y-4 shadow-sm">
            <div>
              <h3 className="text-sm font-semibold text-gray-900">
                {String(selectedEmail.subject || "")}
              </h3>
              <p className="text-xs text-gray-500 mt-1">
                To: {String(selectedEmail.to_email || "")}
              </p>
            </div>

            <div className="bg-gray-50 rounded-lg p-4 max-h-80 overflow-y-auto">
              <div
                className="text-sm text-gray-700 prose prose-sm"
                dangerouslySetInnerHTML={{
                  __html: DOMPurify.sanitize(
                    String(selectedEmail.body || "")
                  ),
                }}
              />
            </div>

            {selectedEmail.status === "draft" && (
              <div className="flex gap-2">
                <button
                  onClick={() =>
                    handleApproveEmail(String(selectedEmail.id))
                  }
                  className="flex-1 flex items-center justify-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 rounded-lg text-sm font-medium text-white transition-colors"
                >
                  <Send className="w-4 h-4" />
                  Approve & Send
                </button>
              </div>
            )}

            {selectedEmail.sent_at ? (
              <div className="flex items-center gap-2 text-xs text-gray-500">
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
