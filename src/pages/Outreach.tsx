import { useEffect, useState, useCallback, useMemo, useRef } from "react";
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
  Square,
  ChevronDown,
  ChevronRight,
  CheckSquare,
  X,
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
  stopPipeline,
  deleteEmails,
  sendApprovedEmails,
  retryFailedEmails,
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
  const [templates, setTemplates] = useState<EmailTemplate[]>([]);
  const [selectedEmail, setSelectedEmail] = useState<Record<
    string,
    unknown
  > | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshCount, setRefreshCount] = useState<number | null>(null);
  const [approving, setApproving] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [sending, setSending] = useState(false);
  const [retrying, setRetrying] = useState(false);
  const [sendResult, setSendResult] = useState<{ sent: number; failed: number } | null>(null);
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());

  useEffect(() => {
    loadEmails();
    loadTemplates();
  }, []);

  async function loadTemplates() {
    try {
      const data = await getEmailTemplates();
      setTemplates(data);
    } catch {
      // templates are optional context — don't block the queue
    }
  }

  // Map template_id → template name
  const templateNames = useMemo(() => {
    const map = new Map<string, string>();
    for (const t of templates) map.set(t.id, t.name);
    return map;
  }, [templates]);

  // Group emails by template_id (preserving order within groups)
  const emailGroups = useMemo(() => {
    const groups: { key: string; label: string; emails: Record<string, unknown>[] }[] = [];
    const groupMap = new Map<string, Record<string, unknown>[]>();
    const order: string[] = [];

    for (const email of emails) {
      const tid = String(email.template_id || "");
      const key = tid || "__none__";
      if (!groupMap.has(key)) {
        groupMap.set(key, []);
        order.push(key);
      }
      groupMap.get(key)!.push(email);
    }

    for (const key of order) {
      const label = key === "__none__"
        ? "Manual / Legacy"
        : templateNames.get(key) || `Template ${key.slice(0, 8)}...`;
      groups.push({ key, label, emails: groupMap.get(key)! });
    }

    return groups;
  }, [emails, templateNames]);

  function toggleGroup(key: string) {
    setCollapsedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }

  // Flat list of visible (non-collapsed) emails for keyboard nav
  const visibleEmails = useMemo(() => {
    const flat: Record<string, unknown>[] = [];
    for (const group of emailGroups) {
      if (!collapsedGroups.has(group.key)) {
        flat.push(...group.emails);
      }
    }
    return flat;
  }, [emailGroups, collapsedGroups]);

  // Ref for scrolling selected row into view
  const listRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      // Don't intercept if user is typing in an input/textarea
      const tag = (e.target as HTMLElement)?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;

      if (e.key === "ArrowDown" || e.key === "j") {
        e.preventDefault();
        setSelectedEmail((prev) => {
          if (!prev) return visibleEmails[0] ?? null;
          const idx = visibleEmails.findIndex((em) => em.id === prev.id);
          const next = visibleEmails[idx + 1];
          return next ?? prev;
        });
      } else if (e.key === "ArrowUp" || e.key === "k") {
        e.preventDefault();
        setSelectedEmail((prev) => {
          if (!prev) return visibleEmails[visibleEmails.length - 1] ?? null;
          const idx = visibleEmails.findIndex((em) => em.id === prev.id);
          const next = visibleEmails[idx - 1];
          return next ?? prev;
        });
      } else if (e.key === "Delete" || e.key === "Backspace") {
        e.preventDefault();
        handleDeleteSelected();
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [visibleEmails]);

  // Scroll selected email row into view
  useEffect(() => {
    if (!selectedEmail || !listRef.current) return;
    const row = listRef.current.querySelector(`[data-email-id="${String(selectedEmail.id)}"]`);
    if (row) row.scrollIntoView({ block: "nearest" });
  }, [selectedEmail]);

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

  function toggleSelect(id: string) {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  function toggleSelectAll(groupEmails: Record<string, unknown>[]) {
    const groupIds = groupEmails.map((e) => String(e.id));
    const allSelected = groupIds.every((id) => selectedIds.has(id));
    setSelectedIds((prev) => {
      const next = new Set(prev);
      for (const id of groupIds) {
        if (allSelected) next.delete(id);
        else next.add(id);
      }
      return next;
    });
  }

  async function handleDeleteSelected() {
    if (deleting || selectedIds.size === 0) return;
    setDeleting(true);
    try {
      await deleteEmails(Array.from(selectedIds));
      setSelectedIds(new Set());
      setSelectedEmail((prev) => {
        if (prev && selectedIds.has(String(prev.id))) return null;
        return prev;
      });
      await loadEmails();
    } catch (e) {
      showError(`Failed to delete emails: ${e}`);
    }
    setDeleting(false);
  }

  async function handleDeleteSingle(id: string) {
    if (deleting) return;
    setDeleting(true);
    try {
      await deleteEmails([id]);
      setSelectedEmail((prev) => {
        if (prev && String(prev.id) === id) return null;
        return prev;
      });
      setSelectedIds((prev) => {
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
      await loadEmails();
    } catch (e) {
      showError(`Failed to delete email: ${e}`);
    }
    setDeleting(false);
  }

  async function handleApproveSelected() {
    if (approving || selectedIds.size === 0) return;
    setApproving(true);
    try {
      for (const id of selectedIds) {
        await updateEmailStatus(id, "approved");
      }
      setSelectedIds(new Set());
      await loadEmails();
    } catch (e) {
      showError(`Failed to approve emails: ${e}`);
    }
    setApproving(false);
  }

  const approvedCount = useMemo(
    () => emails.filter((e) => e.status === "approved").length,
    [emails]
  );

  const failedCount = useMemo(
    () => emails.filter((e) => e.status === "failed").length,
    [emails]
  );

  async function handleRetryFailed() {
    if (retrying) return;
    setRetrying(true);
    try {
      await retryFailedEmails();
      await loadEmails();
    } catch (e) {
      showError(`Failed to retry emails: ${e}`);
    }
    setRetrying(false);
  }

  async function handleSendApproved() {
    if (sending) return;
    setSending(true);
    setSendResult(null);
    try {
      const result = await sendApprovedEmails();
      setSendResult({ sent: result.sent, failed: result.failed });
      await loadEmails();
    } catch (e) {
      showError(`Failed to send emails: ${e}`);
    }
    setSending(false);
  }

  // Sync selectedIds after loadEmails — drop stale IDs
  useEffect(() => {
    const currentIds = new Set(emails.map((e) => String(e.id)));
    setSelectedIds((prev) => {
      const next = new Set<string>();
      for (const id of prev) {
        if (currentIds.has(id)) next.add(id);
      }
      if (next.size === prev.size) return prev;
      return next;
    });
  }, [emails]);

  return (
    <>
      <div className="flex justify-end">
        <div className="flex items-center gap-2">
          {approvedCount > 0 && (
            <button
              onClick={handleSendApproved}
              disabled={sending}
              className="flex items-center gap-2 px-3 py-1.5 bg-green-600 hover:bg-green-700 disabled:opacity-50 rounded-lg text-xs font-medium text-white transition-colors"
            >
              {sending ? (
                <Loader2 className="w-3 h-3 animate-spin" />
              ) : (
                <Send className="w-3 h-3" />
              )}
              {sending ? "Sending..." : `Send ${approvedCount} Approved`}
            </button>
          )}
          {failedCount > 0 && (
            <button
              onClick={handleRetryFailed}
              disabled={retrying}
              className="flex items-center gap-2 px-3 py-1.5 bg-amber-500 hover:bg-amber-600 disabled:opacity-50 rounded-lg text-xs font-medium text-white transition-colors"
            >
              {retrying ? (
                <Loader2 className="w-3 h-3 animate-spin" />
              ) : (
                <RefreshCw className="w-3 h-3" />
              )}
              {retrying ? "Retrying..." : `Retry ${failedCount} Failed`}
            </button>
          )}
          {sendResult != null && (
            <span className="text-xs text-green-600">
              {sendResult.sent} sent{sendResult.failed > 0 ? `, ${sendResult.failed} failed` : ""}
            </span>
          )}
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

      {/* Bulk action bar */}
      {selectedIds.size > 0 && (
        <div className="flex items-center gap-3 px-4 py-2.5 bg-blue-50 border border-blue-200 rounded-lg">
          <CheckSquare className="w-4 h-4 text-blue-600 shrink-0" />
          <span className="text-sm font-medium text-blue-800">
            {selectedIds.size} selected
          </span>
          <div className="flex items-center gap-2 ml-auto">
            <button
              onClick={handleApproveSelected}
              disabled={approving}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-green-600 hover:bg-green-700 disabled:opacity-50 rounded-lg text-xs font-medium text-white transition-colors"
            >
              {approving ? <Loader2 className="w-3 h-3 animate-spin" /> : <Send className="w-3 h-3" />}
              Approve Selected
            </button>
            <button
              onClick={handleDeleteSelected}
              disabled={deleting}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-red-600 hover:bg-red-700 disabled:opacity-50 rounded-lg text-xs font-medium text-white transition-colors"
            >
              {deleting ? <Loader2 className="w-3 h-3 animate-spin" /> : <Trash2 className="w-3 h-3" />}
              Delete Selected
            </button>
            <button
              onClick={() => setSelectedIds(new Set())}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-white border border-gray-200 hover:bg-gray-50 rounded-lg text-xs font-medium text-gray-700 transition-colors"
            >
              <X className="w-3 h-3" />
              Clear
            </button>
          </div>
        </div>
      )}

      <div className="flex gap-4">
        {/* Email list */}
        <div className="flex-1 bg-white rounded-xl border border-gray-200 shadow-sm">
          <div className="p-4 border-b border-gray-200">
            <h2 className="text-sm font-semibold text-gray-900">
              Email Queue ({emails.length})
            </h2>
          </div>

          <div ref={listRef} className="max-h-[calc(100vh-280px)] overflow-y-auto">
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
              emailGroups.map((group) => {
                const isCollapsed = collapsedGroups.has(group.key);
                const statusCounts = group.emails.reduce<Record<string, number>>((acc, e) => {
                  const s = String(e.status || "draft");
                  acc[s] = (acc[s] || 0) + 1;
                  return acc;
                }, {});

                return (
                  <div key={group.key}>
                    {/* Campaign group header */}
                    <div
                      className="flex items-center gap-2 px-4 py-2.5 bg-gray-50 border-b border-gray-200 cursor-pointer hover:bg-gray-100 transition-colors sticky top-0 z-10"
                      onClick={() => toggleGroup(group.key)}
                    >
                      <input
                        type="checkbox"
                        className="w-3.5 h-3.5 rounded border-gray-300 text-blue-600 focus:ring-blue-500 shrink-0"
                        checked={group.emails.length > 0 && group.emails.every((e) => selectedIds.has(String(e.id)))}
                        onChange={(e) => {
                          e.stopPropagation();
                          toggleSelectAll(group.emails);
                        }}
                        onClick={(e) => e.stopPropagation()}
                      />
                      {isCollapsed ? (
                        <ChevronRight className="w-3.5 h-3.5 text-gray-400 shrink-0" />
                      ) : (
                        <ChevronDown className="w-3.5 h-3.5 text-gray-400 shrink-0" />
                      )}
                      <FileText className="w-3.5 h-3.5 text-blue-500 shrink-0" />
                      <span className="text-xs font-semibold text-gray-700 truncate">
                        {group.label}
                      </span>
                      <span className="text-xs text-gray-400 ml-auto shrink-0">
                        {group.emails.length} email{group.emails.length !== 1 ? "s" : ""}
                      </span>
                      <div className="flex gap-1 shrink-0">
                        {Object.entries(statusCounts).map(([status, count]) => (
                          <span
                            key={status}
                            className={`px-1.5 py-0 rounded-full text-[10px] font-medium ${STATUS_COLORS[status] || STATUS_COLORS.draft}`}
                          >
                            {count} {status}
                          </span>
                        ))}
                      </div>
                    </div>

                    {/* Email rows */}
                    {!isCollapsed && (
                      <div className="divide-y divide-gray-100">
                        {group.emails.map((email) => (
                          <div
                            key={String(email.id)}
                            data-email-id={String(email.id)}
                            className={`flex items-center gap-3 px-4 py-3 cursor-pointer transition-colors ${
                              selectedEmail?.id === email.id
                                ? "bg-blue-50"
                                : "hover:bg-gray-50"
                            }`}
                            onClick={() => setSelectedEmail(email)}
                          >
                            <input
                              type="checkbox"
                              className="w-3.5 h-3.5 rounded border-gray-300 text-blue-600 focus:ring-blue-500 shrink-0"
                              checked={selectedIds.has(String(email.id))}
                              onChange={() => toggleSelect(String(email.id))}
                              onClick={(e) => e.stopPropagation()}
                            />
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
                        ))}
                      </div>
                    )}
                  </div>
                );
              })
            )}
          </div>
        </div>

        {/* Email preview — realistic inbox rendering */}
        {selectedEmail && (
          <div className="w-[520px] flex flex-col gap-3">
            {/* Status bar + actions */}
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <span
                  className={`px-2 py-0.5 rounded-full text-xs font-medium ${STATUS_COLORS[String(selectedEmail.status)] || STATUS_COLORS.draft}`}
                >
                  {String(selectedEmail.status || "")}
                </span>
                {typeof selectedEmail.template_id === "string" && selectedEmail.template_id !== "" && (
                  <span className="flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-blue-50 text-blue-700">
                    <FileText className="w-3 h-3" />
                    {templateNames.get(String(selectedEmail.template_id)) || "Template"}
                  </span>
                )}
                {typeof selectedEmail.claim_token === "string" && selectedEmail.claim_token !== "" && (
                  <span className="text-xs text-blue-500">
                    Token: {selectedEmail.claim_token.slice(0, 12)}...
                  </span>
                )}
              </div>
              {selectedEmail.sent_at ? (
                <div className="flex items-center gap-2 text-xs text-gray-500">
                  <Eye className="w-3 h-3" />
                  {"Sent: " + String(selectedEmail.sent_at)}
                  {selectedEmail.opened_at
                    ? " | Opened: " + String(selectedEmail.opened_at)
                    : null}
                </div>
              ) : null}
              {selectedEmail.status === "failed" && typeof selectedEmail.last_error === "string" && selectedEmail.last_error !== "" ? (
                <div className="text-xs text-red-600 bg-red-50 px-2 py-1 rounded max-w-xs truncate" title={selectedEmail.last_error}>
                  {selectedEmail.last_error.slice(0, 80)}
                </div>
              ) : null}
            </div>

            {/* Email client mockup */}
            <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden flex flex-col max-h-[calc(100vh-300px)]">
              {/* Email header — mimics inbox style */}
              <div className="px-5 py-4 border-b border-gray-100">
                <h3 className="text-base font-semibold text-gray-900 leading-snug">
                  {String(selectedEmail.subject || "")}
                </h3>
                <div className="mt-3 flex items-start gap-3">
                  <div className="w-8 h-8 rounded-full bg-blue-600 flex items-center justify-center shrink-0 mt-0.5">
                    <span className="text-white text-xs font-bold">TF</span>
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-baseline gap-2">
                      <span className="text-sm font-medium text-gray-900">Tristan Fischer</span>
                      <span className="text-xs text-gray-400">&lt;tristan@fractionalforge.app&gt;</span>
                    </div>
                    <p className="text-xs text-gray-500 mt-0.5">
                      to {String(selectedEmail.to_email || "")}
                    </p>
                  </div>
                </div>
              </div>

              {/* Email body — white background like a real email */}
              <div className="flex-1 overflow-y-auto px-5 py-5">
                <div
                  className="text-sm text-gray-800 leading-relaxed [&_p]:mb-3 [&_ul]:mb-3 [&_ul]:pl-5 [&_ul]:list-disc [&_li]:mb-1 [&_a]:text-blue-600 [&_a]:underline [&_strong]:font-semibold"
                  dangerouslySetInnerHTML={{
                    __html: DOMPurify.sanitize(
                      String(selectedEmail.body || "")
                    ),
                  }}
                />
              </div>
            </div>

            {/* Action buttons below the email */}
            {selectedEmail.status === "draft" && (
              <div className="flex gap-2">
                <button
                  onClick={() =>
                    handleApproveEmail(String(selectedEmail.id))
                  }
                  disabled={approving}
                  className="flex-1 flex items-center justify-center gap-2 px-4 py-2.5 bg-green-600 hover:bg-green-700 disabled:opacity-50 rounded-lg text-sm font-medium text-white transition-colors"
                >
                  <Send className="w-4 h-4" />
                  Approve & Send
                </button>
                <button
                  onClick={() =>
                    handleDeleteSingle(String(selectedEmail.id))
                  }
                  disabled={deleting}
                  className="flex items-center justify-center gap-2 px-4 py-2.5 bg-red-600 hover:bg-red-700 disabled:opacity-50 rounded-lg text-sm font-medium text-white transition-colors"
                >
                  <Trash2 className="w-4 h-4" />
                  Delete
                </button>
              </div>
            )}
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
  const [stopping, setStopping] = useState(false);
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

  async function handleStop() {
    setStopping(true);
    try {
      await stopPipeline();
    } catch (e) {
      showError(`Failed to stop: ${e}`);
    }
    setStopping(false);
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
                sending ? (
                  <button
                    onClick={handleStop}
                    disabled={stopping}
                    className="flex items-center gap-1.5 px-3 py-1.5 bg-red-600 hover:bg-red-700 disabled:opacity-50 rounded-lg text-xs font-medium text-white transition-colors"
                  >
                    {stopping ? (
                      <Loader2 className="w-3 h-3 animate-spin" />
                    ) : (
                      <Square className="w-3 h-3" />
                    )}
                    Stop Generating
                  </button>
                ) : (
                  <button
                    onClick={() => setShowConfirm(true)}
                    className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 hover:bg-blue-700 rounded-lg text-xs font-medium text-white transition-colors"
                  >
                    <Play className="w-3 h-3" />
                    Generate Drafts
                  </button>
                )
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
            /* Preview mode — realistic email client mockup */
            <div className="p-6 space-y-3">
              <p className="text-xs text-gray-400">
                Preview with sample data — this is how the email will appear in the recipient's inbox.
              </p>

              {/* Email client mockup */}
              <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
                {/* Email header */}
                <div className="px-5 py-4 border-b border-gray-100">
                  <h3 className="text-base font-semibold text-gray-900 leading-snug">
                    {previewSubject}
                  </h3>
                  <div className="mt-3 flex items-start gap-3">
                    <div className="w-8 h-8 rounded-full bg-blue-600 flex items-center justify-center shrink-0 mt-0.5">
                      <span className="text-white text-xs font-bold">TF</span>
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-baseline gap-2">
                        <span className="text-sm font-medium text-gray-900">Tristan Fischer</span>
                        <span className="text-xs text-gray-400">&lt;tristan@fractionalforge.app&gt;</span>
                      </div>
                      <p className="text-xs text-gray-500 mt-0.5">
                        to hans.mueller@acme-manufacturing.de
                      </p>
                    </div>
                  </div>
                </div>

                {/* Email body */}
                <div className="px-5 py-5 max-h-[calc(100vh-480px)] overflow-y-auto">
                  <div
                    className="text-sm text-gray-800 leading-relaxed [&_p]:mb-3 [&_ul]:mb-3 [&_ul]:pl-5 [&_ul]:list-disc [&_li]:mb-1 [&_a]:text-blue-600 [&_a]:underline [&_strong]:font-semibold"
                    dangerouslySetInnerHTML={{
                      __html: DOMPurify.sanitize(previewBody),
                    }}
                  />
                </div>
              </div>
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
