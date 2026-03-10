import { useEffect, useState, useMemo, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { MapContainer, TileLayer, Marker, Popup, useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet.heat";
import { getCompaniesForMap, geocodeCompanies, type MapCompany } from "../lib/tauri";
import { MapPin, RefreshCw, Filter, X } from "lucide-react";
import { useError } from "../contexts/ErrorContext";

// Subcategory color map
const SUBCATEGORY_COLORS: Record<string, string> = {
  "CNC Machining": "#2563eb",
  "CNC Machining - Aerospace": "#1d4ed8",
  "Sheet Metal Fabrication": "#16a34a",
  "Injection Molding & Plastics": "#9333ea",
  "Casting & Forging": "#dc2626",
  "3D Printing & Additive Mfg": "#f59e0b",
  "Electronics Manufacturing": "#06b6d4",
  "Composites & Advanced Materials": "#ec4899",
  "Welding & Structural Steel": "#78716c",
  "Toolmaking & Mould Making": "#ea580c",
  "Medical Device Manufacturing": "#0891b2",
  "Precision Grinding & Lapping": "#65a30d",
  "Waterjet & Laser Cutting": "#e11d48",
};

const DEFAULT_COLOR = "#6b7280";

function createMarkerIcon(color: string): L.DivIcon {
  return L.divIcon({
    className: "custom-marker",
    html: `<div style="
      width: 12px; height: 12px;
      background: ${color};
      border: 2px solid white;
      border-radius: 50%;
      box-shadow: 0 1px 3px rgba(0,0,0,0.3);
    "></div>`,
    iconSize: [12, 12],
    iconAnchor: [6, 6],
    popupAnchor: [0, -8],
  });
}

// Cluster nearby markers manually via grid-based grouping
interface Cluster {
  lat: number;
  lng: number;
  companies: MapCompany[];
}

function clusterMarkers(companies: MapCompany[], zoom: number): Cluster[] {
  if (zoom >= 12) {
    // At high zoom, show individual markers
    return companies.map((c) => ({ lat: c.latitude, lng: c.longitude, companies: [c] }));
  }

  // Grid-based clustering — cell size decreases with zoom
  const cellSize = 180 / Math.pow(2, zoom);
  const grid: Record<string, MapCompany[]> = {};

  for (const c of companies) {
    const key = `${Math.floor(c.latitude / cellSize)}_${Math.floor(c.longitude / cellSize)}`;
    if (!grid[key]) grid[key] = [];
    grid[key].push(c);
  }

  return Object.values(grid).map((group) => {
    const lat = group.reduce((s, c) => s + c.latitude, 0) / group.length;
    const lng = group.reduce((s, c) => s + c.longitude, 0) / group.length;
    return { lat, lng, companies: group };
  });
}

function createClusterIcon(count: number): L.DivIcon {
  const size = count < 10 ? 28 : count < 50 ? 34 : 40;
  const bg = count < 10 ? "#3b82f6" : count < 50 ? "#f59e0b" : "#ef4444";
  return L.divIcon({
    className: "cluster-marker",
    html: `<div style="
      width: ${size}px; height: ${size}px;
      background: ${bg};
      color: white;
      border: 2px solid white;
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 11px;
      font-weight: 600;
      box-shadow: 0 2px 6px rgba(0,0,0,0.3);
    ">${count}</div>`,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  });
}

function ZoomTracker({ onZoomChange }: { onZoomChange: (z: number) => void }) {
  const map = useMap();
  useEffect(() => {
    const handler = () => onZoomChange(map.getZoom());
    map.on("zoomend", handler);
    return () => { map.off("zoomend", handler); };
  }, [map, onZoomChange]);
  return null;
}

function FlyToCluster({ cluster, version }: { cluster: Cluster | null; version: number }) {
  const map = useMap();
  useEffect(() => {
    if (cluster && cluster.companies.length > 1) {
      const bounds = L.latLngBounds(
        cluster.companies.map((c) => [c.latitude, c.longitude] as [number, number])
      );
      map.flyToBounds(bounds, { padding: [50, 50], maxZoom: 14 });
    }
  }, [version, map]);
  return null;
}

const COUNTRY_BOUNDARIES = {
  type: "FeatureCollection",
  features: [
    { type: "Feature", properties: { name: "Germany" }, geometry: { type: "Polygon", coordinates: [[[5.87, 47.27], [5.87, 55.06], [15.04, 55.06], [15.04, 47.27], [5.87, 47.27]]] } },
    { type: "Feature", properties: { name: "France" }, geometry: { type: "Polygon", coordinates: [[[-5.14, 42.33], [-5.14, 51.09], [9.56, 51.09], [9.56, 42.33], [-5.14, 42.33]]] } },
    { type: "Feature", properties: { name: "Netherlands" }, geometry: { type: "Polygon", coordinates: [[[3.36, 50.75], [3.36, 53.47], [7.21, 53.47], [7.21, 50.75], [3.36, 50.75]]] } },
    { type: "Feature", properties: { name: "Belgium" }, geometry: { type: "Polygon", coordinates: [[[2.54, 49.50], [2.54, 51.50], [6.40, 51.50], [6.40, 49.50], [2.54, 49.50]]] } },
    { type: "Feature", properties: { name: "Italy" }, geometry: { type: "Polygon", coordinates: [[[6.63, 36.65], [6.63, 47.09], [18.52, 47.09], [18.52, 36.65], [6.63, 36.65]]] } },
    { type: "Feature", properties: { name: "United Kingdom" }, geometry: { type: "Polygon", coordinates: [[[-8.17, 49.96], [-8.17, 58.64], [1.75, 58.64], [1.75, 49.96], [-8.17, 49.96]]] } },
  ],
};

function HeatmapLayer({ companies }: { companies: MapCompany[] }) {
  const map = useMap();
  useEffect(() => {
    const points: [number, number, number][] = companies.map((c) => [
      c.latitude,
      c.longitude,
      (c.relevance_score ?? 50) / 100,
    ]);
    const heat = L.heatLayer(points, { radius: 25, blur: 15, maxZoom: 10 });
    heat.addTo(map);
    return () => { map.removeLayer(heat); };
  }, [map, companies]);
  return null;
}

function BoundaryLayer({ visible }: { visible: boolean }) {
  const map = useMap();
  useEffect(() => {
    if (!visible) return;
    const layer = L.geoJSON(COUNTRY_BOUNDARIES as any, {
      style: { fillColor: "#93c5fd", fillOpacity: 0.1, color: "#93c5fd", weight: 1 },
    });
    layer.addTo(map);
    return () => { map.removeLayer(layer); };
  }, [map, visible]);
  return null;
}

export default function MapPage() {
  const navigate = useNavigate();
  const { showError, showInfo } = useError();
  const [companies, setCompanies] = useState<MapCompany[]>([]);
  const [loading, setLoading] = useState(true);
  const [geocoding, setGeocoding] = useState(false);
  const [zoom, setZoom] = useState(6);
  const [flyTarget, setFlyTarget] = useState<Cluster | null>(null);
  const [flyVersion, setFlyVersion] = useState(0);
  const [filterOpen, setFilterOpen] = useState(false);
  const [selectedCategories, setSelectedCategories] = useState<Set<string>>(new Set());
  const [minRelevance, setMinRelevance] = useState(0);
  const [mapMode, setMapMode] = useState<"markers" | "heatmap">("markers");
  const [showBoundaries, setShowBoundaries] = useState(false);

  useEffect(() => {
    loadCompanies();
  }, []);

  async function loadCompanies() {
    setLoading(true);
    try {
      const data = await getCompaniesForMap();
      setCompanies(data);
    } catch (e) {
      console.error("Failed to load map companies:", e);
    }
    setLoading(false);
  }

  async function handleGeocode() {
    setGeocoding(true);
    try {
      const result = await geocodeCompanies();
      showInfo(`Geocoded ${result.geocoded} of ${result.total} companies (${result.failed} failed)`);
      loadCompanies();
    } catch (e) {
      showError(`Geocoding error: ${e}`);
    }
    setGeocoding(false);
  }

  // Get unique subcategories for filter
  const subcategories = useMemo(() => {
    const set = new Set<string>();
    companies.forEach((c) => {
      if (c.subcategory) set.add(c.subcategory);
    });
    return Array.from(set).sort();
  }, [companies]);

  // Apply filters
  const filtered = useMemo(() => {
    return companies.filter((c) => {
      if (selectedCategories.size > 0 && (!c.subcategory || !selectedCategories.has(c.subcategory))) {
        return false;
      }
      if (minRelevance > 0 && (c.relevance_score ?? 0) < minRelevance) {
        return false;
      }
      return true;
    });
  }, [companies, selectedCategories, minRelevance]);

  // Cluster markers based on zoom
  const clusters = useMemo(() => clusterMarkers(filtered, zoom), [filtered, zoom]);

  const handleZoomChange = useCallback((z: number) => setZoom(z), []);

  const toggleCategory = (cat: string) => {
    setSelectedCategories((prev) => {
      const next = new Set(prev);
      if (next.has(cat)) next.delete(cat);
      else next.add(cat);
      return next;
    });
  };

  return (
    <div className="h-full flex flex-col -m-6">
      {/* Header bar */}
      <div className="flex items-center justify-between px-4 py-2 bg-white border-b border-gray-200">
        <div className="flex items-center gap-2">
          <MapPin className="w-4 h-4 text-forge-600" />
          <h1 className="text-sm font-semibold text-gray-900">Map</h1>
          <span className="text-xs text-gray-500">
            {filtered.length} companies with coordinates
          </span>
        </div>
        <div className="flex items-center gap-2">
          <div className="flex items-center gap-1 border border-gray-300 rounded overflow-hidden">
            <button
              onClick={() => setMapMode("markers")}
              className={`px-2 py-1 text-xs ${mapMode === "markers" ? "bg-forge-600 text-white" : "text-gray-600 hover:bg-gray-50"}`}
            >
              Markers
            </button>
            <button
              onClick={() => setMapMode("heatmap")}
              className={`px-2 py-1 text-xs ${mapMode === "heatmap" ? "bg-forge-600 text-white" : "text-gray-600 hover:bg-gray-50"}`}
            >
              Heatmap
            </button>
          </div>
          <button
            onClick={() => setFilterOpen(!filterOpen)}
            className={`flex items-center gap-1 px-2 py-1 text-xs rounded border transition-colors ${
              filterOpen || selectedCategories.size > 0 || minRelevance > 0
                ? "bg-forge-50 text-forge-700 border-forge-300"
                : "text-gray-600 border-gray-300 hover:bg-gray-50"
            }`}
          >
            <Filter className="w-3 h-3" />
            Filters
            {(selectedCategories.size > 0 || minRelevance > 0) && (
              <span className="ml-1 px-1 bg-forge-600 text-white rounded-full text-[10px]">
                {selectedCategories.size + (minRelevance > 0 ? 1 : 0)}
              </span>
            )}
          </button>
          <button
            onClick={handleGeocode}
            disabled={geocoding}
            className="flex items-center gap-1 px-2 py-1 text-xs text-gray-600 border border-gray-300 rounded hover:bg-gray-50 disabled:opacity-50"
          >
            <RefreshCw className={`w-3 h-3 ${geocoding ? "animate-spin" : ""}`} />
            {geocoding ? "Geocoding..." : "Backfill Geocodes"}
          </button>
        </div>
      </div>

      {/* Filter panel */}
      {filterOpen && (
        <div className="px-4 py-3 bg-white border-b border-gray-200 space-y-3">
          <div className="flex items-center justify-between">
            <span className="text-xs font-medium text-gray-700">Filter by Category</span>
            {selectedCategories.size > 0 && (
              <button
                onClick={() => setSelectedCategories(new Set())}
                className="text-[10px] text-forge-600 hover:underline"
              >
                Clear all
              </button>
            )}
          </div>
          <div className="flex flex-wrap gap-1">
            {subcategories.map((cat) => {
              const color = SUBCATEGORY_COLORS[cat] || DEFAULT_COLOR;
              const selected = selectedCategories.has(cat);
              return (
                <button
                  key={cat}
                  onClick={() => toggleCategory(cat)}
                  className={`flex items-center gap-1 px-2 py-0.5 text-[10px] rounded-full border transition-colors ${
                    selected
                      ? "border-gray-600 bg-gray-100 text-gray-900 font-medium"
                      : "border-gray-200 text-gray-500 hover:border-gray-300"
                  }`}
                >
                  <span
                    className="w-2 h-2 rounded-full inline-block"
                    style={{ background: color }}
                  />
                  {cat}
                </button>
              );
            })}
          </div>
          <div className="flex items-center gap-2">
            <span className="text-xs text-gray-700">Min Relevance:</span>
            <input
              type="range"
              min={0}
              max={100}
              value={minRelevance}
              onChange={(e) => setMinRelevance(Number(e.target.value))}
              className="w-40 h-1 accent-forge-600"
            />
            <span className="text-xs text-gray-500 w-8">{minRelevance}</span>
            {minRelevance > 0 && (
              <button
                onClick={() => setMinRelevance(0)}
                className="text-gray-400 hover:text-gray-600"
              >
                <X className="w-3 h-3" />
              </button>
            )}
          </div>
          <label className="flex items-center gap-2 text-xs text-gray-700">
            <input type="checkbox" checked={showBoundaries} onChange={(e) => setShowBoundaries(e.target.checked)} className="accent-forge-600" />
            Show country boundaries
          </label>
        </div>
      )}

      {/* Map */}
      <div className="flex-1 relative">
        {loading ? (
          <div className="absolute inset-0 flex items-center justify-center bg-gray-50">
            <div className="text-sm text-gray-500">Loading map data...</div>
          </div>
        ) : (
          <MapContainer
            center={[54.5, -2.5]}
            zoom={6}
            className="h-full w-full"
            scrollWheelZoom={true}
          >
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />
            <ZoomTracker onZoomChange={handleZoomChange} />
            <FlyToCluster cluster={flyTarget} version={flyVersion} />
            <BoundaryLayer visible={showBoundaries} />

            {mapMode === "heatmap" ? (
              <HeatmapLayer companies={filtered} />
            ) : clusters.map((cluster, i) => {
              if (cluster.companies.length === 1) {
                const c = cluster.companies[0];
                const color = SUBCATEGORY_COLORS[c.subcategory ?? ""] || DEFAULT_COLOR;
                return (
                  <Marker
                    key={c.id}
                    position={[c.latitude, c.longitude]}
                    icon={createMarkerIcon(color)}
                  >
                    <Popup>
                      <div className="text-xs space-y-1 min-w-[180px]">
                        <div className="font-semibold text-gray-900">{c.name}</div>
                        {c.subcategory && (
                          <div className="flex items-center gap-1">
                            <span
                              className="w-2 h-2 rounded-full inline-block"
                              style={{ background: color }}
                            />
                            <span className="text-gray-600">{c.subcategory}</span>
                          </div>
                        )}
                        {c.city && (
                          <div className="text-gray-500">{c.city}</div>
                        )}
                        {c.relevance_score != null && (
                          <div className="text-gray-500">
                            Relevance: {c.relevance_score}
                          </div>
                        )}
                        {c.website_url && (
                          <a
                            href={c.website_url.startsWith("http") ? c.website_url : `https://${c.website_url}`}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-forge-600 hover:underline block truncate"
                          >
                            {c.website_url}
                          </a>
                        )}
                        <button
                          onClick={() => navigate(`/review?search=${encodeURIComponent(c.name)}`)}
                          className="text-forge-600 hover:underline text-[10px] mt-1 block"
                        >
                          View in Review →
                        </button>
                      </div>
                    </Popup>
                  </Marker>
                );
              }

              // Cluster marker
              return (
                <Marker
                  key={`cluster-${i}`}
                  position={[cluster.lat, cluster.lng]}
                  icon={createClusterIcon(cluster.companies.length)}
                  eventHandlers={{
                    click: () => {
                      setFlyTarget(cluster);
                      setFlyVersion((v) => v + 1);
                    },
                  }}
                >
                  <Popup>
                    <div className="text-xs space-y-1 min-w-[160px] max-h-[200px] overflow-y-auto">
                      <div className="font-semibold text-gray-900">
                        {cluster.companies.length} companies
                      </div>
                      {cluster.companies.slice(0, 10).map((c) => (
                        <div key={c.id} className="text-gray-600 truncate">
                          {c.name}
                        </div>
                      ))}
                      {cluster.companies.length > 10 && (
                        <div className="text-gray-400">
                          +{cluster.companies.length - 10} more
                        </div>
                      )}
                      <div className="text-[10px] text-gray-400 pt-1">
                        Click to zoom in
                      </div>
                    </div>
                  </Popup>
                </Marker>
              );
            })}
          </MapContainer>
        )}
      </div>
    </div>
  );
}
