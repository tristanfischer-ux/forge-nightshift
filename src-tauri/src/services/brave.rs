use anyhow::Result;
use serde::{Deserialize, Serialize};

const BRAVE_SEARCH_URL: &str = "https://api.search.brave.com/res/v1/web/search";

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResult {
    pub title: String,
    pub url: String,
    pub description: String,
}

#[derive(Debug, Deserialize)]
struct BraveResponse {
    web: Option<WebResults>,
}

#[derive(Debug, Deserialize)]
struct WebResults {
    results: Vec<BraveResult>,
}

#[derive(Debug, Deserialize)]
struct BraveResult {
    title: String,
    url: String,
    description: String,
}

pub struct SearchCategory {
    pub id: &'static str,
    pub name: &'static str,
    pub keywords: &'static [&'static str],
}

pub const CATEGORIES: &[SearchCategory] = &[
    SearchCategory { id: "cnc_machining", name: "CNC Machining", keywords: &["CNC machining", "precision turned parts", "5-axis milling"] },
    SearchCategory { id: "cnc_aerospace", name: "CNC Machining - Aerospace", keywords: &["aerospace machining", "AS9100 CNC", "flight-critical parts"] },
    SearchCategory { id: "sheet_metal", name: "Sheet Metal Fabrication", keywords: &["sheet metal fabrication", "laser cutting service", "metal forming"] },
    SearchCategory { id: "injection_molding", name: "Injection Molding & Plastics", keywords: &["injection moulding", "rubber molding", "plastic parts manufacturer"] },
    SearchCategory { id: "casting_forging", name: "Casting & Forging", keywords: &["investment casting", "die casting", "forging foundry"] },
    SearchCategory { id: "3d_printing", name: "3D Printing & Additive Mfg", keywords: &["3D printing service", "additive manufacturing", "SLS DMLS printing"] },
    SearchCategory { id: "electronics", name: "Electronics Manufacturing", keywords: &["PCB assembly", "EMS contract electronics", "electronic manufacturing"] },
    SearchCategory { id: "composites", name: "Composites & Advanced Materials", keywords: &["carbon fibre manufacturer", "GFRP CFRP composites", "composite parts"] },
    SearchCategory { id: "welding_steel", name: "Welding & Structural Steel", keywords: &["welding fabrication", "structural steel", "metal stamping"] },
    SearchCategory { id: "springs_fasteners", name: "Springs, Fasteners & Gears", keywords: &["spring manufacturer", "precision gears", "custom fasteners"] },
    SearchCategory { id: "surface_treatment", name: "Surface Treatment & Finishing", keywords: &["anodising plating", "heat treatment service", "powder coating"] },
    SearchCategory { id: "contract_assembly", name: "Assembly & Contract Manufacturing", keywords: &["contract assembly", "turnkey manufacturing", "box build assembly"] },
    SearchCategory { id: "optics_photonics", name: "Precision Optics & Photonics", keywords: &["optical components manufacturer", "photonics company", "precision lens sensor"] },
    SearchCategory { id: "hydraulics", name: "Hydraulics & Pneumatics", keywords: &["hydraulic systems", "pneumatic actuators", "fluid power"] },
    SearchCategory { id: "motors_drives", name: "Motors, Drives & Power Electronics", keywords: &["electric motors manufacturer", "servo drives", "VFD power electronics"] },
    SearchCategory { id: "bearings_motion", name: "Bearings & Linear Motion", keywords: &["bearings manufacturer", "linear guides", "motion control seals"] },
    SearchCategory { id: "process_vessels", name: "Process Vessels & Pharma Equipment", keywords: &["stainless vessels", "pharma equipment manufacturer", "GMP tanks"] },
    SearchCategory { id: "valves_pumps", name: "Valves, Pumps & Flow Control", keywords: &["industrial valves", "pumps manufacturer", "filtration systems"] },
    SearchCategory { id: "automation_robotics", name: "Automation & Robotics Integration", keywords: &["automation systems integrator", "robot integrator PLC", "control panels"] },
    SearchCategory { id: "connectors_cabling", name: "Connectors, Cabling & Magnetics", keywords: &["electrical connectors", "HV cable manufacturer", "transformers magnetics"] },
    SearchCategory { id: "battery_energy", name: "Battery & Energy Components", keywords: &["battery components", "energy storage manufacturer", "renewable equipment"] },
    SearchCategory { id: "ceramics_glass", name: "Ceramics, Glass & Specialty Coatings", keywords: &["advanced ceramics", "industrial glass manufacturer", "specialty coatings"] },
    SearchCategory { id: "thermal_management", name: "Thermal Management & Heat Exchangers", keywords: &["heat exchangers manufacturer", "cooling systems", "thermal management"] },
    SearchCategory { id: "testing_ndt", name: "Testing, Inspection & NDT", keywords: &["NDT services", "environmental testing", "metrology CMM"] },
    SearchCategory { id: "ai_compute", name: "AI Infrastructure & Compute Hardware", keywords: &["AI chip manufacturer", "compute hardware", "AI inference hardware"] },
    SearchCategory { id: "quantum", name: "Quantum Computing & Technology", keywords: &["quantum computing company", "quantum sensor", "cryogenics equipment"] },
    SearchCategory { id: "robotics_autonomous", name: "Robotics & Autonomous Systems", keywords: &["robotics company", "cobot manufacturer", "AMR autonomous robot"] },
    SearchCategory { id: "cleantech", name: "Cleantech & Energy Hardware", keywords: &["cleantech manufacturer", "solar panel manufacturer", "wind turbine electrolyzer"] },
    SearchCategory { id: "space_tech", name: "Space Technology & Satellites", keywords: &["satellite manufacturer", "space components", "launch technology"] },
    SearchCategory { id: "toolmaking", name: "Toolmaking & Mould Making", keywords: &["toolmaker", "mould maker", "die manufacturer jig fixture"] },
    SearchCategory { id: "wire_cable", name: "Wire & Cable Manufacturing", keywords: &["wire manufacturer", "cable assembly", "wire harness"] },
    SearchCategory { id: "packaging_machinery", name: "Packaging Machinery & Systems", keywords: &["packaging machinery", "filling machine", "labelling equipment"] },
    SearchCategory { id: "precision_grinding", name: "Precision Grinding & Lapping", keywords: &["precision grinding", "cylindrical grinding", "surface lapping"] },
    SearchCategory { id: "waterjet_laser", name: "Waterjet & Laser Cutting", keywords: &["waterjet cutting", "laser profiling", "CNC plasma cutting"] },
    SearchCategory { id: "rubber_seals", name: "Rubber, Seals & Gaskets", keywords: &["rubber moulding", "O-ring manufacturer", "gasket manufacturer"] },
    SearchCategory { id: "filtration_separation", name: "Filtration & Separation Equipment", keywords: &["industrial filtration", "separation equipment", "filter element"] },
    SearchCategory { id: "medical_devices", name: "Medical Device Manufacturing", keywords: &["medical device", "surgical instrument", "implant manufacturer"] },
];

pub async fn test_connection(api_key: &str) -> Result<bool> {
    let client = reqwest::Client::new();
    let resp = client
        .get(BRAVE_SEARCH_URL)
        .header("X-Subscription-Token", api_key)
        .query(&[("q", "test"), ("count", "1")])
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    Ok(resp.status().is_success())
}

pub async fn search(api_key: &str, query: &str, country: &str, count: u32) -> Result<Vec<SearchResult>> {
    let search_lang = search_lang_for_country(country);
    let client = reqwest::Client::new();
    let resp = client
        .get(BRAVE_SEARCH_URL)
        .header("X-Subscription-Token", api_key)
        .query(&[
            ("q", query),
            ("count", &count.to_string()),
            ("country", country),
            ("search_lang", search_lang),
        ])
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Brave Search error {}: {}", status, body);
    }

    let brave_resp: BraveResponse = resp.json().await?;
    let results = brave_resp
        .web
        .map(|w| {
            w.results
                .into_iter()
                .map(|r| SearchResult {
                    title: r.title,
                    url: r.url,
                    description: r.description,
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(results)
}

fn country_names(country: &str) -> Vec<&'static str> {
    match country {
        "DE" => vec!["Germany", "Deutschland"],
        "FR" => vec!["France"],
        "NL" => vec!["Netherlands", "Nederland"],
        "BE" => vec!["Belgium"],
        "IT" => vec!["Italy", "Italia"],
        "GB" | "UK" => vec!["UK", "United Kingdom", "Britain"],
        _ => vec![],
    }
}

/// Return the Brave search_lang parameter for a country code.
fn search_lang_for_country(country: &str) -> &'static str {
    match country {
        "DE" => "de",
        "FR" => "fr",
        "NL" => "nl",
        "IT" => "it",
        "BE" => "fr", // Belgium — French is the more common web language
        "GB" | "UK" => "en",
        _ => "en",
    }
}

/// Native-language manufacturing terms for non-English countries.
fn native_terms(country: &str) -> Vec<&'static str> {
    match country {
        "DE" => vec!["Hersteller", "Fertigung", "Bearbeitung", "Maschinenbau"],
        "FR" => vec!["fabricant", "usinage", "fabrication", "mécanique"],
        "NL" => vec!["fabrikant", "machinefabriek", "metaalbewerking"],
        "IT" => vec!["produttore", "lavorazione", "fabbricazione", "meccanica"],
        "BE" => vec!["fabricant", "fabrikant", "usinage", "metaalbewerking"],
        _ => vec![],
    }
}

/// Native country name for query building.
fn native_country_name(country: &str) -> Option<&'static str> {
    match country {
        "DE" => Some("Deutschland"),
        "FR" => Some("France"),
        "NL" => Some("Nederland"),
        "IT" => Some("Italia"),
        "BE" => Some("België"),
        _ => None,
    }
}

/// Generate search queries for a given country and category.
/// Returns Vec<(query_string, category_id)>.
pub fn generate_queries_for_category(country: &str, category: &SearchCategory) -> Vec<(String, String)> {
    let names = country_names(country);
    if names.is_empty() {
        return vec![];
    }

    let mut queries = Vec::new();
    let cat_id = category.id.to_string();

    // Use first country name for primary queries
    let primary = names[0];

    for keyword in category.keywords {
        queries.push((
            format!("{} manufacturer {}", keyword, primary),
            cat_id.clone(),
        ));
    }

    // If there's an alternate name, add one extra query with it
    if names.len() > 1 {
        let alt = names[1];
        queries.push((
            format!("{} company {}", category.keywords[0], alt),
            cat_id.clone(),
        ));
    }

    // Native-language queries (1-2 per category)
    let terms = native_terms(country);
    if let Some(native_name) = native_country_name(country) {
        if !terms.is_empty() {
            // Use first keyword's core concept + native manufacturer term + native country
            queries.push((
                format!("{} {} {}", category.keywords[0], terms[0], native_name),
                cat_id.clone(),
            ));
            // Second variant if we have enough terms
            if terms.len() > 1 {
                queries.push((
                    format!("{} {} {}", category.keywords[0], terms[1], native_name),
                    cat_id.clone(),
                ));
            }
        }
    }

    queries
}
