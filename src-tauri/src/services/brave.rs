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

/// Owned version of SearchCategory for dynamic profiles loaded from DB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicSearchCategory {
    pub id: String,
    pub name: String,
    pub keywords: Vec<String>,
}

/// Return domain-appropriate role words for query building.
pub fn get_role_words_for_domain(domain: &str) -> Vec<&'static str> {
    match domain {
        "manufacturing" => vec!["manufacturer", "supplier", "factory", "producer", "fabricator"],
        "cleantech" => vec!["company", "provider", "solutions", "developer", "installer"],
        "biotech" => vec!["company", "laboratory", "research", "developer", "pharmaceutical"],
        _ => vec!["company", "provider", "business", "service", "solutions"],
    }
}

/// Generate search queries for a dynamic (DB-loaded) category.
pub fn generate_queries_for_dynamic_category(country: &str, category: &DynamicSearchCategory, domain: &str) -> Vec<(String, String)> {
    let names = country_names(country);
    if names.is_empty() {
        return vec![];
    }

    let role_words = get_role_words_for_domain(domain);
    let mut queries = Vec::new();
    let cat_id = category.id.clone();
    let primary = names[0];

    for (i, keyword) in category.keywords.iter().enumerate() {
        let has_role = keyword_has_role_suffix(keyword);

        let templates_count = if i < 3 {
            role_words.len() // all
        } else if i < 7 {
            2.min(role_words.len())
        } else {
            1.min(role_words.len())
        };

        for template_role in &role_words[..templates_count] {
            let query = if has_role {
                format!("{} {}", keyword, primary)
            } else {
                format!("{} {} {}", keyword, template_role, primary)
            };
            queries.push((query, cat_id.clone()));
        }

        if (country == "GB" || country == "UK") && i < 3 {
            queries.push((
                format!("{} ltd {}", keyword, primary),
                cat_id.clone(),
            ));
        }
    }

    // Alternate country name queries
    for alt in names.iter().skip(1) {
        if let Some(keyword) = category.keywords.first() {
            if keyword_has_role_suffix(keyword) {
                queries.push((format!("{} {}", keyword, alt), cat_id.clone()));
            } else {
                queries.push((format!("{} company {}", keyword, alt), cat_id.clone()));
            }
        }
    }

    // UK regional queries
    if country == "GB" || country == "UK" {
        let regional_keywords = category.keywords.iter().take(3);
        for keyword in regional_keywords {
            for region in UK_REGIONS {
                let query = if keyword_has_role_suffix(keyword) {
                    format!("{} {}", keyword, region)
                } else {
                    let role = role_words.first().unwrap_or(&"company");
                    format!("{} {} {}", keyword, role, region)
                };
                queries.push((query, cat_id.clone()));
            }
        }
    }

    // Native-language queries for non-English countries
    let terms = native_terms(country);
    if let Some(native_name) = native_country_name(country) {
        if !terms.is_empty() {
            if let Some(keyword) = category.keywords.first() {
                queries.push((
                    format!("{} {} {}", keyword, terms[0], native_name),
                    cat_id.clone(),
                ));
                if terms.len() > 1 {
                    queries.push((
                        format!("{} {} {}", keyword, terms[1], native_name),
                        cat_id.clone(),
                    ));
                }
            }
        }
    }

    queries
}

pub const CATEGORIES: &[SearchCategory] = &[
    SearchCategory { id: "cnc_machining", name: "CNC Machining", keywords: &[
        "CNC machining", "precision turned parts", "5-axis milling",
        "CNC turning service", "Swiss screw machining", "CNC milling subcontractor",
        "precision engineering", "CNC prototype parts", "multi-axis machining", "CNC job shop",
    ]},
    SearchCategory { id: "cnc_aerospace", name: "CNC Machining - Aerospace", keywords: &[
        "aerospace machining", "AS9100 CNC", "flight-critical parts",
        "aerospace precision components", "aircraft parts machining", "turbine blade machining",
        "defence machining", "aerospace subcontract machining", "NADCAP machining", "aerospace turned parts",
    ]},
    SearchCategory { id: "sheet_metal", name: "Sheet Metal Fabrication", keywords: &[
        "sheet metal fabrication", "laser cutting service", "metal forming",
        "sheet metal enclosures", "metal stampings", "press brake forming",
        "thin gauge fabrication", "aluminium sheet metal", "stainless sheet work", "prototype sheet metal",
    ]},
    SearchCategory { id: "injection_molding", name: "Injection Molding & Plastics", keywords: &[
        "injection moulding", "rubber molding", "plastic parts manufacturer",
        "thermoplastic moulding", "overmoulding service", "insert moulding",
        "plastic injection tooling", "medical grade moulding", "nylon moulding", "polymer processing",
    ]},
    SearchCategory { id: "casting_forging", name: "Casting & Forging", keywords: &[
        "investment casting", "die casting", "forging foundry",
        "sand casting foundry", "aluminium casting", "precision lost wax casting",
        "closed die forging", "centrifugal casting", "gravity die casting", "iron foundry",
    ]},
    SearchCategory { id: "3d_printing", name: "3D Printing & Additive Mfg", keywords: &[
        "3D printing service", "additive manufacturing", "SLS DMLS printing",
        "metal 3D printing", "rapid prototyping service", "SLA printing service",
        "polymer additive manufacturing", "3D printing bureau", "binder jetting service", "FDM production parts",
    ]},
    SearchCategory { id: "electronics", name: "Electronics Manufacturing", keywords: &[
        "PCB assembly", "EMS contract electronics", "electronic manufacturing",
        "PCBA service", "surface mount assembly", "through-hole assembly",
        "electronic box build", "PCB design and assembly", "prototype PCB assembly", "cable harness assembly",
    ]},
    SearchCategory { id: "composites", name: "Composites & Advanced Materials", keywords: &[
        "carbon fibre manufacturer", "GFRP CFRP composites", "composite parts",
        "prepreg composites", "filament winding", "composite tooling",
        "aerospace composites", "structural composites", "composite moulding", "resin transfer moulding",
    ]},
    SearchCategory { id: "welding_steel", name: "Welding & Structural Steel", keywords: &[
        "welding fabrication", "structural steel", "metal stamping",
        "TIG welding service", "MIG welding fabrication", "steel fabrication company",
        "heavy fabrication", "aluminium welding", "stainless steel fabrication", "coded welding",
    ]},
    SearchCategory { id: "springs_fasteners", name: "Springs, Fasteners & Gears", keywords: &[
        "spring manufacturer", "precision gears", "custom fasteners",
        "compression spring manufacturer", "bespoke fasteners", "gear cutting service",
        "wire forming", "torsion spring manufacturer", "special fasteners", "worm gear manufacturer",
    ]},
    SearchCategory { id: "surface_treatment", name: "Surface Treatment & Finishing", keywords: &[
        "anodising plating", "heat treatment service", "powder coating",
        "electroplating service", "hard anodising", "zinc plating",
        "shot blasting service", "phosphating", "passivation service", "thermal spraying",
    ]},
    SearchCategory { id: "contract_assembly", name: "Assembly & Contract Manufacturing", keywords: &[
        "contract assembly", "turnkey manufacturing", "box build assembly",
        "mechanical assembly service", "sub-assembly service", "kitting and assembly",
        "electromechanical assembly", "clean room assembly", "batch assembly", "jig and fixture assembly",
    ]},
    SearchCategory { id: "optics_photonics", name: "Precision Optics & Photonics", keywords: &[
        "optical components manufacturer", "photonics company", "precision lens sensor",
        "optical coating service", "fibre optic manufacturer", "laser components",
        "infrared optics", "optical assemblies", "photonic devices", "precision mirrors",
    ]},
    SearchCategory { id: "hydraulics", name: "Hydraulics & Pneumatics", keywords: &[
        "hydraulic systems", "pneumatic actuators", "fluid power",
        "hydraulic cylinder manufacturer", "pneumatic valves", "hydraulic power pack",
        "bespoke hydraulic systems", "pneumatic conveying", "hydraulic hose assembly", "pneumatic cylinder",
    ]},
    SearchCategory { id: "motors_drives", name: "Motors, Drives & Power Electronics", keywords: &[
        "electric motors manufacturer", "servo drives", "VFD power electronics",
        "brushless motor manufacturer", "motor winding", "stepper motor",
        "power converter manufacturer", "drive systems", "electric actuator", "motor control electronics",
    ]},
    SearchCategory { id: "bearings_motion", name: "Bearings & Linear Motion", keywords: &[
        "bearings manufacturer", "linear guides", "motion control seals",
        "precision bearings", "ball screw manufacturer", "linear actuator",
        "needle roller bearings", "cam follower manufacturer", "slewing bearings", "bearing refurbishment",
    ]},
    SearchCategory { id: "process_vessels", name: "Process Vessels & Pharma Equipment", keywords: &[
        "stainless vessels", "pharma equipment manufacturer", "GMP tanks",
        "pressure vessel manufacturer", "mixing vessels", "jacketed vessels",
        "pharmaceutical stainless steel", "process piping", "reactor vessel", "CIP systems",
    ]},
    SearchCategory { id: "valves_pumps", name: "Valves, Pumps & Flow Control", keywords: &[
        "industrial valves", "pumps manufacturer", "filtration systems",
        "control valves manufacturer", "diaphragm pump", "positive displacement pump",
        "ball valve manufacturer", "check valve manufacturer", "solenoid valve", "metering pump",
    ]},
    SearchCategory { id: "automation_robotics", name: "Automation & Robotics Integration", keywords: &[
        "automation systems integrator", "robot integrator PLC", "control panels",
        "factory automation", "SCADA systems", "machine vision systems",
        "robotic welding", "PLC programming", "automated test equipment", "conveyor systems",
    ]},
    SearchCategory { id: "connectors_cabling", name: "Connectors, Cabling & Magnetics", keywords: &[
        "electrical connectors", "HV cable manufacturer", "transformers magnetics",
        "connector manufacturer", "cable assembly", "magnetic components",
        "RF connectors", "power transformers", "toroidal transformer", "coil winding",
    ]},
    SearchCategory { id: "battery_energy", name: "Battery & Energy Components", keywords: &[
        "battery components", "energy storage manufacturer", "renewable equipment",
        "battery pack assembly", "fuel cell components", "power electronics",
        "inverter manufacturer", "battery management system", "supercapacitor", "energy harvesting",
    ]},
    SearchCategory { id: "ceramics_glass", name: "Ceramics, Glass & Specialty Coatings", keywords: &[
        "advanced ceramics", "industrial glass manufacturer", "specialty coatings",
        "technical ceramics", "alumina ceramics", "zirconia components",
        "borosilicate glass", "ceramic machining", "glass-to-metal seals", "thin film coating",
    ]},
    SearchCategory { id: "thermal_management", name: "Thermal Management & Heat Exchangers", keywords: &[
        "heat exchangers manufacturer", "cooling systems", "thermal management",
        "plate heat exchanger", "heat sink manufacturer", "thermal interface materials",
        "industrial cooling", "finned tube heat exchanger", "thermal simulation", "cold plate manufacturer",
    ]},
    SearchCategory { id: "testing_ndt", name: "Testing, Inspection & NDT", keywords: &[
        "NDT services", "environmental testing", "metrology CMM",
        "radiographic inspection", "ultrasonic testing service", "UKAS calibration",
        "coordinate measuring machine", "materials testing lab", "fatigue testing", "dimensional inspection",
    ]},
    SearchCategory { id: "ai_compute", name: "AI Infrastructure & Compute Hardware", keywords: &[
        "AI chip manufacturer", "compute hardware", "AI inference hardware",
        "GPU server manufacturer", "edge computing hardware", "AI accelerator",
        "FPGA systems", "high-performance computing", "machine learning hardware", "data centre hardware",
    ]},
    SearchCategory { id: "quantum", name: "Quantum Computing & Technology", keywords: &[
        "quantum computing company", "quantum sensor", "cryogenics equipment",
        "quantum components", "dilution refrigerator", "superconducting electronics",
        "quantum photonics", "quantum sensing", "cryostat manufacturer", "quantum key distribution",
    ]},
    SearchCategory { id: "robotics_autonomous", name: "Robotics & Autonomous Systems", keywords: &[
        "robotics company", "cobot manufacturer", "AMR autonomous robot",
        "industrial robot systems", "drone manufacturer", "AGV manufacturer",
        "robotic arms", "autonomous vehicles", "robot end effectors", "mobile robots",
    ]},
    SearchCategory { id: "cleantech", name: "Cleantech & Energy Hardware", keywords: &[
        "cleantech manufacturer", "solar panel manufacturer", "wind turbine electrolyzer",
        "heat pump manufacturer", "hydrogen electrolyser", "solar inverter",
        "wind turbine components", "biomass equipment", "tidal energy", "carbon capture equipment",
    ]},
    SearchCategory { id: "space_tech", name: "Space Technology & Satellites", keywords: &[
        "satellite manufacturer", "space components", "launch technology",
        "small satellite manufacturer", "space grade electronics", "CubeSat components",
        "rocket propulsion", "satellite subsystems", "space mechanisms", "radiation hardened electronics",
    ]},
    SearchCategory { id: "toolmaking", name: "Toolmaking & Mould Making", keywords: &[
        "toolmaker", "mould maker", "die manufacturer jig fixture",
        "injection mould toolmaker", "press tool manufacturer", "precision tooling",
        "extrusion die maker", "gauge and fixture", "EDM toolmaking", "tool and die shop",
    ]},
    SearchCategory { id: "wire_cable", name: "Wire & Cable Manufacturing", keywords: &[
        "wire manufacturer", "cable assembly", "wire harness",
        "loom manufacturer", "cable harness assembly", "specialist wire",
        "multicore cable manufacturer", "bespoke cable assembly", "coaxial cable manufacturer", "ribbon cable assembly",
    ]},
    SearchCategory { id: "packaging_machinery", name: "Packaging Machinery & Systems", keywords: &[
        "packaging machinery", "filling machine", "labelling equipment",
        "packaging automation", "cartoning machine", "shrink wrap machine",
        "blister packaging", "weighing and packing", "palletising systems", "form fill seal",
    ]},
    SearchCategory { id: "precision_grinding", name: "Precision Grinding & Lapping", keywords: &[
        "precision grinding", "cylindrical grinding", "surface lapping",
        "centreless grinding", "jig grinding", "creep feed grinding",
        "diamond grinding", "honing service", "flat lapping", "thread grinding",
    ]},
    SearchCategory { id: "waterjet_laser", name: "Waterjet & Laser Cutting", keywords: &[
        "waterjet cutting", "laser profiling", "CNC plasma cutting",
        "abrasive waterjet cutting", "fibre laser cutting", "tube laser cutting",
        "metal profiling service", "laser engraving service", "flame cutting", "precision laser cutting",
    ]},
    SearchCategory { id: "rubber_seals", name: "Rubber, Seals & Gaskets", keywords: &[
        "rubber moulding", "O-ring manufacturer", "gasket manufacturer",
        "silicone moulding", "rubber extrusion", "hydraulic seal manufacturer",
        "rubber to metal bonding", "custom seals", "PTFE seals", "bespoke gaskets",
    ]},
    SearchCategory { id: "filtration_separation", name: "Filtration & Separation Equipment", keywords: &[
        "industrial filtration", "separation equipment", "filter element",
        "dust extraction systems", "oil filtration", "HEPA filter manufacturer",
        "membrane filtration", "centrifugal separator", "coalescence filtration", "process filtration",
    ]},
    SearchCategory { id: "medical_devices", name: "Medical Device Manufacturing", keywords: &[
        "medical device", "surgical instrument", "implant manufacturer",
        "ISO 13485 manufacturer", "orthopaedic implants", "medical precision machining",
        "cleanroom medical manufacturing", "catheter manufacturer", "diagnostic equipment", "dental implant manufacturer",
    ]},
];

/// Role words that, if already present as a suffix in a keyword, should not be
/// re-appended by the template engine.
const ROLE_WORDS: &[&str] = &[
    "manufacturer", "company", "supplier", "factory", "producer",
    "service", "services", "fabrication", "foundry", "integrator",
    "maker", "shop", "subcontractor",
];


/// UK manufacturing hub locations for regional queries.
const UK_REGIONS: &[&str] = &[
    "West Midlands", "East Midlands", "North West England", "Yorkshire",
    "South East England", "South Wales", "Scotland", "North East England",
    "Bristol", "Sheffield", "Birmingham", "Coventry", "Manchester", "Leeds",
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

pub async fn search(api_key: &str, query: &str, country: &str, count: u32, offset: u32) -> Result<Vec<SearchResult>> {
    let search_lang = search_lang_for_country(country);
    let client = reqwest::Client::new();

    let mut params: Vec<(&str, String)> = vec![
        ("q", query.to_string()),
        ("count", count.to_string()),
        ("country", country.to_string()),
        ("search_lang", search_lang.to_string()),
    ];
    if offset > 0 {
        params.push(("offset", offset.to_string()));
    }

    let resp = client
        .get(BRAVE_SEARCH_URL)
        .header("X-Subscription-Token", api_key)
        .query(&params)
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

pub fn country_names(country: &str) -> Vec<&'static str> {
    match country {
        "DE" => vec!["Germany", "Deutschland"],
        "FR" => vec!["France"],
        "NL" => vec!["Netherlands", "Nederland"],
        "BE" => vec!["Belgium"],
        "IT" => vec!["Italy", "Italia"],
        "GB" | "UK" => vec!["UK", "United Kingdom", "England", "Britain", "Great Britain"],
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

/// Check if a keyword already ends with a role word (e.g. "plastic parts manufacturer").
fn keyword_has_role_suffix(keyword: &str) -> bool {
    let lower = keyword.to_lowercase();
    ROLE_WORDS.iter().any(|role| lower.ends_with(role))
}

