import { useEffect, useState, useCallback } from "react";
import {
  Mail,
  Send,
  Eye,
  RefreshCw,
  Loader2,
  FileText,
  Plus,
  Trash2,
  Play,
  Save,
} from "lucide-react";
import DOMPurify from "dompurify";
import {
  getEmails,
  updateEmailStatus,
  refreshEmailStatuses,
  getEmailTemplates,
  saveEmailTemplate,
  deleteEmailTemplate,
  getCampaignEligibleCount,
  startPipeline,
  EmailTemplate,
} from "../lib/tauri";
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

type Tab = "emails" | "templates";

export default function Outreach() {
  const { showError } = useError();
  const [tab, setTab] = useState<Tab>("emails");

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Outreach</h1>
          <p className="text-sm text-gray-500 mt-1">
            Email drafts, templates, and send tracking
          </p>
        </div>
        <div className="flex items-center gap-1 bg-gray-100 rounded-lg p-0.5">
          <button
            onClick={() => setTab("emails")}
            className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
              tab === "emails"
                ? "bg-white text-gray-900 shadow-sm"
                : "text-gray-500 hover:text-gray-700"
            }`}
          >
            <Mail className="w-3 h-3 inline mr-1" />
            Email Queue
          </button>
          <button
            onClick={() => setTab("templates")}
            className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
              tab === "templates"
                ? "bg-white text-gray-900 shadow-sm"
                : "text-gray-500 hover:text-gray-700"
            }`}
          >
            <FileText className="w-3 h-3 inline mr-1" />
            Templates
          </button>
        </div>
      </div>

      {tab === "emails" ? (
        <EmailQueueTab showError={showError} />
      ) : (
        <TemplatesTab showError={showError} />
      )}
    </div>
  );
}

// --- Email Queue Tab (existing functionality) ---

function EmailQueueTab({ showError }: { showError: (msg: string) => void }) {
  const [emails, setEmails] = useState<Record<string, unknown>[]>([]);
  const [selectedEmail, setSelectedEmail] = useState<Record<
    string,
    unknown
  > | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshCount, setRefreshCount] = useState<number | null>(null);
  const [approving, setApproving] = useState(false);

  useEffect(() => {
    loadEmails();
  }, []);

  async function loadEmails() {
    setLoading(true);
    try {
      const data = await getEmails(undefined, 100);
      setEmails(data);
      setSelectedEmail((prev) => {
        if (!prev) return null;
        return data.find((e) => e.id === prev.id) ?? null;
      });
    } catch (e) {
      showError(`Failed to load emails: ${e}`);
    }
    setLoading(false);
  }

  async function handleApproveEmail(id: string) {
    if (approving) return;
    setApproving(true);
    try {
      await updateEmailStatus(id, "approved");
      await loadEmails();
    } catch (e) {
      showError(`Failed to approve email: ${e}`);
    }
    setApproving(false);
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
    <>
      <div className="flex justify-end">
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

          <div className="divide-y divide-gray-100 max-h-[calc(100vh-280px)] overflow-y-auto">
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
              {typeof selectedEmail.claim_token === "string" && selectedEmail.claim_token !== "" && (
                <p className="text-xs text-blue-500 mt-0.5">
                  Claim token: {selectedEmail.claim_token.slice(0, 12)}...
                </p>
              )}
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
                  disabled={approving}
                  className="flex-1 flex items-center justify-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 disabled:opacity-50 rounded-lg text-sm font-medium text-white transition-colors"
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
    </>
  );
}

// --- Templates Tab ---

const DEFAULT_TEMPLATE_BODY = `<p>Hi {contact_name},</p>

<p>I came across <strong>{company_name}</strong> and was impressed by your manufacturing capabilities.</p>

<p>We're building <strong>ForgeOS</strong> — a marketplace that connects manufacturers with engineers who need production partners. Companies on our platform get:</p>

<ul>
  <li><strong>Marketplace visibility</strong> — engineers search by capability, location, and certification</li>
  <li><strong>Fractional executive income</strong> — monetise spare capacity and expertise</li>
  <li><strong>Facility bookings</strong> — fill downtime with on-demand production runs</li>
</ul>

<p>We've already created a listing for {company_name}. You can <strong>claim and customise it</strong> in under 2 minutes:</p>

<p><a href="{claim_url}" style="display:inline-block;padding:10px 24px;background:#2563eb;color:#ffffff;text-decoration:none;border-radius:6px;font-weight:600;">Claim Your Listing</a></p>

<p>No cost, no commitment — just verify your details and you're live.</p>

<p>Best regards,<br/>The ForgeOS Team</p>

<p style="font-size:11px;color:#9ca3af;">If you'd prefer not to hear from us, simply ignore this email.</p>`;

function TemplatesTab({ showError }: { showError: (msg: string) => void }) {
  const [templates, setTemplates] = useState<EmailTemplate[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [eligibleCount, setEligibleCount] = useState<number | null>(null);
  const [sending, setSending] = useState(false);
  const [showConfirm, setShowConfirm] = useState(false);

  // Editor state
  const [editName, setEditName] = useState("");
  const [editSubject, setEditSubject] = useState("");
  const [editBody, setEditBody] = useState("");
  const [isNew, setIsNew] = useState(false);
  const [showPreview, setShowPreview] = useState(false);

  const loadTemplates = useCallback(async () => {
    setLoading(true);
    try {
      const data = await getEmailTemplates();
      setTemplates(data);
    } catch (e) {
      showError(`Failed to load templates: ${e}`);
    }
    setLoading(false);
  }, [showError]);

  const loadEligibleCount = useCallback(async () => {
    try {
      const count = await getCampaignEligibleCount();
      setEligibleCount(count);
    } catch {
      setEligibleCount(null);
    }
  }, []);

  useEffect(() => {
    loadTemplates();
    loadEligibleCount();
  }, [loadTemplates, loadEligibleCount]);

  function handleSelectTemplate(t: EmailTemplate) {
    setSelectedId(t.id);
    setEditName(t.name);
    setEditSubject(t.subject);
    setEditBody(t.body);
    setIsNew(false);
    setShowPreview(false);
  }

  function handleNewTemplate() {
    setSelectedId(null);
    setEditName("Default Outreach");
    setEditSubject("Claim your {company_name} listing on ForgeOS");
    setEditBody(DEFAULT_TEMPLATE_BODY);
    setIsNew(true);
    setShowPreview(false);
  }

  async function handleSave() {
    if (saving) return;
    setSaving(true);
    try {
      const result = await saveEmailTemplate({
        id: isNew ? undefined : (selectedId ?? undefined),
        name: editName,
        subject: editSubject,
        body: editBody,
      });
      if (result.created) {
        setSelectedId(result.id);
        setIsNew(false);
      }
      await loadTemplates();
    } catch (e) {
      showError(`Failed to save template: ${e}`);
    }
    setSaving(false);
  }

  async function handleDelete(id: string) {
    try {
      await deleteEmailTemplate(id);
      if (selectedId === id) {
        setSelectedId(null);
        setEditName("");
        setEditSubject("");
        setEditBody("");
      }
      await loadTemplates();
    } catch (e) {
      showError(`Failed to delete template: ${e}`);
    }
  }

  async function handleSendCampaign() {
    if (!selectedId || sending) return;
    setSending(true);
    setShowConfirm(false);
    try {
      await startPipeline([`template_outreach:${selectedId}`]);
    } catch (e) {
      showError(`Failed to start campaign: ${e}`);
    }
    setSending(false);
  }

  // Preview with sample data
  const previewSubject = editSubject
    .replace(/{company_name}/g, "Acme Manufacturing GmbH")
    .replace(/{contact_name}/g, "Hans Mueller")
    .replace(/{claim_url}/g, "https://fractionalforge.app/claim/abc123def456...");
  const previewBody = editBody
    .replace(/{company_name}/g, "Acme Manufacturing GmbH")
    .replace(/{contact_name}/g, "Hans Mueller")
    .replace(/{claim_url}/g, "https://fractionalforge.app/claim/abc123def456...");

  const hasEditor = selectedId !== null || isNew;

  return (
    <div className="flex gap-4">
      {/* Template list */}
      <div className="w-64 bg-white rounded-xl border border-gray-200 shadow-sm shrink-0">
        <div className="p-4 border-b border-gray-200 flex items-center justify-between">
          <h2 className="text-sm font-semibold text-gray-900">Templates</h2>
          <button
            onClick={handleNewTemplate}
            className="p-1 rounded hover:bg-gray-100 text-gray-500 transition-colors"
            title="New template"
          >
            <Plus className="w-4 h-4" />
          </button>
        </div>

        <div className="divide-y divide-gray-100 max-h-[calc(100vh-280px)] overflow-y-auto">
          {loading ? (
            <div className="flex items-center justify-center p-8">
              <Loader2 className="w-5 h-5 text-gray-400 animate-spin" />
            </div>
          ) : templates.length === 0 && !isNew ? (
            <div className="p-6 text-center text-gray-400 text-sm">
              No templates yet.
              <button
                onClick={handleNewTemplate}
                className="block mx-auto mt-2 text-blue-600 hover:text-blue-700 text-xs"
              >
                Create one
              </button>
            </div>
          ) : (
            templates.map((t) => (
              <div
                key={t.id}
                className={`flex items-center gap-2 px-4 py-3 cursor-pointer transition-colors ${
                  selectedId === t.id && !isNew
                    ? "bg-blue-50"
                    : "hover:bg-gray-50"
                }`}
                onClick={() => handleSelectTemplate(t)}
              >
                <FileText className="w-4 h-4 text-gray-400 shrink-0" />
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-gray-900 truncate">
                    {t.name}
                  </p>
                  <p className="text-xs text-gray-500 truncate">{t.subject}</p>
                </div>
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    handleDelete(t.id);
                  }}
                  className="p-1 rounded hover:bg-red-50 text-gray-300 hover:text-red-500 transition-colors"
                >
                  <Trash2 className="w-3 h-3" />
                </button>
              </div>
            ))
          )}
        </div>

        {/* Eligible count */}
        {eligibleCount !== null && (
          <div className="p-4 border-t border-gray-200">
            <p className="text-xs text-gray-500">
              <span className="font-semibold text-gray-700">{eligibleCount}</span>{" "}
              companies eligible
            </p>
          </div>
        )}
      </div>

      {/* Editor / Preview */}
      {hasEditor ? (
        <div className="flex-1 bg-white rounded-xl border border-gray-200 shadow-sm">
          {/* Editor header */}
          <div className="p-4 border-b border-gray-200 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <h2 className="text-sm font-semibold text-gray-900">
                {isNew ? "New Template" : "Edit Template"}
              </h2>
              <div className="flex items-center gap-1 bg-gray-100 rounded-md p-0.5 ml-2">
                <button
                  onClick={() => setShowPreview(false)}
                  className={`px-2 py-0.5 rounded text-xs transition-colors ${
                    !showPreview
                      ? "bg-white text-gray-900 shadow-sm"
                      : "text-gray-500"
                  }`}
                >
                  Edit
                </button>
                <button
                  onClick={() => setShowPreview(true)}
                  className={`px-2 py-0.5 rounded text-xs transition-colors ${
                    showPreview
                      ? "bg-white text-gray-900 shadow-sm"
                      : "text-gray-500"
                  }`}
                >
                  Preview
                </button>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={handleSave}
                disabled={saving}
                className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-900 hover:bg-gray-800 disabled:opacity-50 rounded-lg text-xs font-medium text-white transition-colors"
              >
                {saving ? (
                  <Loader2 className="w-3 h-3 animate-spin" />
                ) : (
                  <Save className="w-3 h-3" />
                )}
                Save
              </button>
              {!isNew && selectedId && (
                <button
                  onClick={() => setShowConfirm(true)}
                  disabled={sending}
                  className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 rounded-lg text-xs font-medium text-white transition-colors"
                >
                  {sending ? (
                    <Loader2 className="w-3 h-3 animate-spin" />
                  ) : (
                    <Play className="w-3 h-3" />
                  )}
                  Generate Drafts
                </button>
              )}
            </div>
          </div>

          {/* Confirm dialog */}
          {showConfirm && (
            <div className="mx-4 mt-4 p-4 bg-amber-50 border border-amber-200 rounded-lg">
              <p className="text-sm text-amber-800 font-medium">
                Generate personalised drafts for {eligibleCount ?? "?"} companies?
              </p>
              <p className="text-xs text-amber-600 mt-1">
                Ollama will personalise each email using company data. You'll
                review each draft before sending.
              </p>
              <div className="flex gap-2 mt-3">
                <button
                  onClick={handleSendCampaign}
                  className="px-3 py-1.5 bg-blue-600 hover:bg-blue-700 rounded-lg text-xs font-medium text-white transition-colors"
                >
                  Generate Drafts
                </button>
                <button
                  onClick={() => setShowConfirm(false)}
                  className="px-3 py-1.5 bg-white border border-gray-200 hover:bg-gray-50 rounded-lg text-xs font-medium text-gray-700 transition-colors"
                >
                  Cancel
                </button>
              </div>
            </div>
          )}

          {showPreview ? (
            /* Preview mode */
            <div className="p-6 space-y-4">
              <div>
                <p className="text-xs text-gray-500 mb-1">Subject</p>
                <p className="text-sm font-medium text-gray-900">
                  {previewSubject}
                </p>
              </div>
              <div>
                <p className="text-xs text-gray-500 mb-1">
                  To: hans.mueller@acme-manufacturing.de
                </p>
              </div>
              <div className="bg-gray-50 rounded-lg p-6 max-h-[calc(100vh-420px)] overflow-y-auto">
                <div
                  className="text-sm text-gray-700 prose prose-sm"
                  dangerouslySetInnerHTML={{
                    __html: DOMPurify.sanitize(previewBody),
                  }}
                />
              </div>
              <p className="text-xs text-gray-400">
                Preview uses sample data: Acme Manufacturing GmbH, Hans Mueller
              </p>
            </div>
          ) : (
            /* Edit mode */
            <div className="p-6 space-y-4">
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">
                  Template Name
                </label>
                <input
                  type="text"
                  value={editName}
                  onChange={(e) => setEditName(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                  placeholder="e.g. Default Outreach"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">
                  Subject Line
                </label>
                <input
                  type="text"
                  value={editSubject}
                  onChange={(e) => setEditSubject(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                  placeholder="e.g. Claim your {company_name} listing on ForgeOS"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">
                  Body (HTML)
                </label>
                <textarea
                  value={editBody}
                  onChange={(e) => setEditBody(e.target.value)}
                  rows={16}
                  className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-500 resize-y"
                  placeholder="HTML email body..."
                />
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs font-medium text-gray-600 mb-1">
                  Available placeholders:
                </p>
                <div className="flex flex-wrap gap-2">
                  {["{company_name}", "{contact_name}", "{claim_url}"].map(
                    (p) => (
                      <code
                        key={p}
                        className="px-2 py-0.5 bg-white border border-gray-200 rounded text-xs text-gray-700"
                      >
                        {p}
                      </code>
                    )
                  )}
                </div>
              </div>
            </div>
          )}
        </div>
      ) : (
        <div className="flex-1 bg-white rounded-xl border border-gray-200 shadow-sm flex items-center justify-center">
          <div className="text-center text-gray-400 p-8">
            <FileText className="w-8 h-8 mx-auto mb-2 opacity-50" />
            <p className="text-sm">Select a template or create a new one</p>
          </div>
        </div>
      )}
    </div>
  );
}
