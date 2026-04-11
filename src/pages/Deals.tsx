import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  Target,
  DollarSign,
  Calendar,
  User,
  Trash2,
  ChevronDown,
  ArrowUpCircle,
} from "lucide-react";
import {
  getDeals,
  updateDealStatus,
  deleteDeal,
  saveDeal,
  type Deal,
} from "../lib/tauri";

const DEAL_STATUSES = [
  "identified",
  "researching",
  "contacted",
  "in_discussion",
  "engaged",
  "closed",
  "passed",
] as const;

const STATUS_LABELS: Record<string, string> = {
  identified: "Identified",
  researching: "Researching",
  contacted: "Contacted",
  in_discussion: "In Discussion",
  engaged: "Engaged",
  closed: "Closed",
  passed: "Passed",
};

const STATUS_COLORS: Record<string, string> = {
  identified: "bg-gray-100 text-gray-700",
  researching: "bg-blue-100 text-blue-700",
  contacted: "bg-yellow-100 text-yellow-700",
  in_discussion: "bg-purple-100 text-purple-700",
  engaged: "bg-orange-100 text-orange-700",
  closed: "bg-green-100 text-green-700",
  passed: "bg-red-100 text-red-700",
};

const PRIORITY_COLORS: Record<string, string> = {
  high: "bg-red-100 text-red-700",
  medium: "bg-yellow-100 text-yellow-700",
  low: "bg-gray-100 text-gray-500",
};

type Tab = "ma_target" | "fundraise_candidate";

export default function Deals() {
  const navigate = useNavigate();
  const [tab, setTab] = useState<Tab>("ma_target");
  const [deals, setDeals] = useState<Deal[]>([]);
  const [loading, setLoading] = useState(true);
  const [filterStatus, setFilterStatus] = useState<string>("all");
  const [filterPriority, setFilterPriority] = useState<string>("all");
  const [filterAssigned, setFilterAssigned] = useState<string>("all");
  const [editingDeal, setEditingDeal] = useState<Deal | null>(null);

  async function loadDeals() {
    setLoading(true);
    try {
      const result = await getDeals(tab);
      setDeals(result);
    } catch (err) {
      console.error("Failed to load deals:", err);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadDeals();
  }, [tab]);

  const filtered = deals.filter((d) => {
    if (filterStatus !== "all" && d.status !== filterStatus) return false;
    if (filterPriority !== "all" && d.priority !== filterPriority) return false;
    if (filterAssigned !== "all" && d.assigned_to !== filterAssigned)
      return false;
    return true;
  });

  async function handleStatusChange(deal: Deal, newStatus: string) {
    try {
      await updateDealStatus(deal.id, newStatus);
      loadDeals();
    } catch (err) {
      console.error("Failed to update deal status:", err);
    }
  }

  async function handleDelete(deal: Deal) {
    if (
      !confirm(
        `Remove ${deal.company_name || deal.company_id} from deal tracking?`
      )
    )
      return;
    try {
      await deleteDeal(deal.id);
      loadDeals();
    } catch (err) {
      console.error("Failed to delete deal:", err);
    }
  }

  async function handleSaveEdit() {
    if (!editingDeal) return;
    try {
      await saveDeal({
        companyId: editingDeal.company_id,
        dealType: editingDeal.deal_type,
        status: editingDeal.status,
        priority: editingDeal.priority,
        notes: editingDeal.notes || undefined,
        assignedTo: editingDeal.assigned_to || undefined,
        estimatedValue: editingDeal.estimated_value || undefined,
        nextAction: editingDeal.next_action || undefined,
        nextActionDate: editingDeal.next_action_date || undefined,
      });
      setEditingDeal(null);
      loadDeals();
    } catch (err) {
      console.error("Failed to save deal:", err);
    }
  }

  // Summary stats
  const activeDeals = deals.filter(
    (d) => !["closed", "passed"].includes(d.status)
  );
  const highPriority = deals.filter((d) => d.priority === "high").length;
  const closedCount = deals.filter((d) => d.status === "closed").length;

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Deal Tracking</h1>
          <p className="text-sm text-gray-500">
            Track M&A targets and fundraise candidates
          </p>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-2 border-b border-gray-200">
        <button
          onClick={() => setTab("ma_target")}
          className={`flex items-center gap-2 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
            tab === "ma_target"
              ? "border-orange-500 text-orange-700"
              : "border-transparent text-gray-500 hover:text-gray-700"
          }`}
        >
          <Target className="w-4 h-4" />
          M&A Targets
        </button>
        <button
          onClick={() => setTab("fundraise_candidate")}
          className={`flex items-center gap-2 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
            tab === "fundraise_candidate"
              ? "border-green-500 text-green-700"
              : "border-transparent text-gray-500 hover:text-gray-700"
          }`}
        >
          <DollarSign className="w-4 h-4" />
          Fundraise Candidates
        </button>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-3">
        <div className="bg-white rounded-xl border border-gray-200 p-4">
          <div className="text-2xl font-bold text-gray-900">
            {activeDeals.length}
          </div>
          <div className="text-xs text-gray-500">Active Deals</div>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-4">
          <div className="text-2xl font-bold text-red-600">{highPriority}</div>
          <div className="text-xs text-gray-500">High Priority</div>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-4">
          <div className="text-2xl font-bold text-green-600">{closedCount}</div>
          <div className="text-xs text-gray-500">Closed</div>
        </div>
      </div>

      {/* Filters */}
      <div className="flex gap-3 items-center">
        <select
          value={filterStatus}
          onChange={(e) => setFilterStatus(e.target.value)}
          className="text-xs border border-gray-200 rounded-lg px-2.5 py-1.5 bg-white"
        >
          <option value="all">All Statuses</option>
          {DEAL_STATUSES.map((s) => (
            <option key={s} value={s}>
              {STATUS_LABELS[s]}
            </option>
          ))}
        </select>
        <select
          value={filterPriority}
          onChange={(e) => setFilterPriority(e.target.value)}
          className="text-xs border border-gray-200 rounded-lg px-2.5 py-1.5 bg-white"
        >
          <option value="all">All Priorities</option>
          <option value="high">High</option>
          <option value="medium">Medium</option>
          <option value="low">Low</option>
        </select>
        <select
          value={filterAssigned}
          onChange={(e) => setFilterAssigned(e.target.value)}
          className="text-xs border border-gray-200 rounded-lg px-2.5 py-1.5 bg-white"
        >
          <option value="all">All Assignees</option>
          <option value="tristan">Tristan</option>
          <option value="fraser">Fraser</option>
        </select>
        <span className="text-xs text-gray-400 ml-auto">
          {filtered.length} deal{filtered.length !== 1 ? "s" : ""}
        </span>
      </div>

      {/* Table */}
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
        {loading ? (
          <div className="p-8 text-center text-gray-400 text-sm">
            Loading deals...
          </div>
        ) : filtered.length === 0 ? (
          <div className="p-8 text-center text-gray-400 text-sm">
            No{" "}
            {tab === "ma_target" ? "M&A targets" : "fundraise candidates"}{" "}
            found.
            <br />
            <span className="text-xs">
              Use the Review page to tag companies as deals.
            </span>
          </div>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 border-b border-gray-200 text-left text-xs text-gray-500 uppercase">
                <th className="px-4 py-2.5">Company</th>
                <th className="px-4 py-2.5">Priority</th>
                <th className="px-4 py-2.5">Status</th>
                <th className="px-4 py-2.5">Assigned</th>
                <th className="px-4 py-2.5">Est. Value</th>
                <th className="px-4 py-2.5">Next Action</th>
                <th className="px-4 py-2.5">Updated</th>
                <th className="px-4 py-2.5 w-16"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {filtered.map((deal) => (
                <tr
                  key={deal.id}
                  className="hover:bg-gray-50 transition-colors"
                >
                  <td className="px-4 py-3">
                    <button
                      className="text-left"
                      onClick={() =>
                        navigate(`/review?company=${deal.company_id}`)
                      }
                    >
                      <div className="font-medium text-gray-900 hover:text-forge-600 transition-colors">
                        {deal.company_name || deal.company_id}
                      </div>
                      <div className="text-xs text-gray-400">
                        {deal.subcategory || ""}{" "}
                        {deal.country ? `/ ${deal.country}` : ""}
                      </div>
                    </button>
                  </td>
                  <td className="px-4 py-3">
                    <span
                      className={`px-2 py-0.5 rounded-full text-xs font-medium ${
                        PRIORITY_COLORS[deal.priority] || ""
                      }`}
                    >
                      {deal.priority}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    <div className="relative inline-block">
                      <select
                        value={deal.status}
                        onChange={(e) =>
                          handleStatusChange(deal, e.target.value)
                        }
                        className={`appearance-none cursor-pointer px-2 py-0.5 rounded-full text-xs font-medium pr-5 ${
                          STATUS_COLORS[deal.status] || ""
                        }`}
                      >
                        {DEAL_STATUSES.map((s) => (
                          <option key={s} value={s}>
                            {STATUS_LABELS[s]}
                          </option>
                        ))}
                      </select>
                      <ChevronDown className="w-3 h-3 absolute right-1 top-1/2 -translate-y-1/2 pointer-events-none text-gray-400" />
                    </div>
                  </td>
                  <td className="px-4 py-3">
                    <span className="text-xs text-gray-600 flex items-center gap-1">
                      <User className="w-3 h-3" />
                      {deal.assigned_to || "-"}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    <span className="text-xs font-medium text-gray-700">
                      {deal.estimated_value || "-"}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    <div className="text-xs text-gray-600">
                      {deal.next_action || "-"}
                    </div>
                    {deal.next_action_date && (
                      <div className="text-[10px] text-gray-400 flex items-center gap-0.5 mt-0.5">
                        <Calendar className="w-2.5 h-2.5" />
                        {deal.next_action_date}
                      </div>
                    )}
                  </td>
                  <td className="px-4 py-3">
                    <span className="text-[10px] text-gray-400">
                      {deal.updated_at
                        ? new Date(deal.updated_at + "Z").toLocaleDateString()
                        : "-"}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    <div className="flex gap-1">
                      <button
                        onClick={() => setEditingDeal({ ...deal })}
                        title="Edit deal"
                        className="p-1 text-gray-400 hover:text-forge-600 transition-colors"
                      >
                        <ArrowUpCircle className="w-3.5 h-3.5" />
                      </button>
                      <button
                        onClick={() => handleDelete(deal)}
                        title="Remove deal"
                        className="p-1 text-gray-400 hover:text-red-600 transition-colors"
                      >
                        <Trash2 className="w-3.5 h-3.5" />
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Edit dialog */}
      {editingDeal && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
          <div className="bg-white rounded-xl shadow-xl w-full max-w-md p-5 space-y-4">
            <h3 className="text-sm font-semibold text-gray-900">
              Edit Deal:{" "}
              {editingDeal.company_name || editingDeal.company_id}
            </h3>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="text-xs text-gray-500">Status</label>
                <select
                  value={editingDeal.status}
                  onChange={(e) =>
                    setEditingDeal({ ...editingDeal, status: e.target.value })
                  }
                  className="w-full text-xs border border-gray-200 rounded-lg px-2.5 py-1.5"
                >
                  {DEAL_STATUSES.map((s) => (
                    <option key={s} value={s}>
                      {STATUS_LABELS[s]}
                    </option>
                  ))}
                </select>
              </div>
              <div>
                <label className="text-xs text-gray-500">Priority</label>
                <select
                  value={editingDeal.priority}
                  onChange={(e) =>
                    setEditingDeal({
                      ...editingDeal,
                      priority: e.target.value,
                    })
                  }
                  className="w-full text-xs border border-gray-200 rounded-lg px-2.5 py-1.5"
                >
                  <option value="high">High</option>
                  <option value="medium">Medium</option>
                  <option value="low">Low</option>
                </select>
              </div>
              <div>
                <label className="text-xs text-gray-500">Assigned To</label>
                <select
                  value={editingDeal.assigned_to || ""}
                  onChange={(e) =>
                    setEditingDeal({
                      ...editingDeal,
                      assigned_to: e.target.value || null,
                    })
                  }
                  className="w-full text-xs border border-gray-200 rounded-lg px-2.5 py-1.5"
                >
                  <option value="">Unassigned</option>
                  <option value="tristan">Tristan</option>
                  <option value="fraser">Fraser</option>
                </select>
              </div>
              <div>
                <label className="text-xs text-gray-500">
                  Estimated Value
                </label>
                <input
                  type="text"
                  value={editingDeal.estimated_value || ""}
                  onChange={(e) =>
                    setEditingDeal({
                      ...editingDeal,
                      estimated_value: e.target.value || null,
                    })
                  }
                  placeholder="e.g. $500K-1M"
                  className="w-full text-xs border border-gray-200 rounded-lg px-2.5 py-1.5"
                />
              </div>
            </div>
            <div>
              <label className="text-xs text-gray-500">Next Action</label>
              <input
                type="text"
                value={editingDeal.next_action || ""}
                onChange={(e) =>
                  setEditingDeal({
                    ...editingDeal,
                    next_action: e.target.value || null,
                  })
                }
                placeholder="e.g. Schedule intro call"
                className="w-full text-xs border border-gray-200 rounded-lg px-2.5 py-1.5"
              />
            </div>
            <div>
              <label className="text-xs text-gray-500">Next Action Date</label>
              <input
                type="date"
                value={editingDeal.next_action_date || ""}
                onChange={(e) =>
                  setEditingDeal({
                    ...editingDeal,
                    next_action_date: e.target.value || null,
                  })
                }
                className="w-full text-xs border border-gray-200 rounded-lg px-2.5 py-1.5"
              />
            </div>
            <div>
              <label className="text-xs text-gray-500">Notes</label>
              <textarea
                rows={3}
                value={editingDeal.notes || ""}
                onChange={(e) =>
                  setEditingDeal({
                    ...editingDeal,
                    notes: e.target.value || null,
                  })
                }
                placeholder="Internal notes about this deal..."
                className="w-full text-xs border border-gray-200 rounded-lg px-2.5 py-1.5 resize-none"
              />
            </div>
            <div className="flex gap-2 justify-end">
              <button
                onClick={() => setEditingDeal(null)}
                className="px-3 py-1.5 text-xs text-gray-600 hover:text-gray-800"
              >
                Cancel
              </button>
              <button
                onClick={handleSaveEdit}
                className="px-4 py-1.5 text-xs bg-forge-600 text-white rounded-lg hover:bg-forge-700"
              >
                Save
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
