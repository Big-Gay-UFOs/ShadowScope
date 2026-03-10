# SAM.gov High-Precision Targeting Package (Evidence-Calibrated)

## Scope and evidence
- Source: `C:\Users\kabyl\Downloads\lead_snapshot_first50_evaluated.xlsx`
- Parsed rows: 50 labeled opportunities (`relevant` or `not relevant`)
- Label assumption in workbook: broader-scoped construction/facilities, engineering, logistics, or fabrication are relevant; commodity parts, niche medical/clinical, event/hospitality, and low-context amendment-only items are not relevant.
- Precision baseline from current ranking:
  - `P@10 = 50%` (5/10 relevant)
  - `P@20 = 40%` (8/20 relevant)
  - Overall relevant rate in first 50 = `18%` (9/50)

## 1) Target profiles / lane definitions

### Lane A: Facilities Modernization and Sustainment (Primary)
- Objective: prioritize opportunities with scoped built-environment work.
- In scope:
  - Renovation, build-out, replacement, expansion
  - HVAC maintenance/repair BPAs
  - Roofing and facility modernization
- Out of scope:
  - Isolated commodity parts for facilities systems
  - Amendment-only items with no discernible scope
- Evidence: 6/9 relevant labels are `construction/facilities`.

### Lane B: Program Logistics and Mission Support Services (Secondary)
- Objective: capture broad service contracts with multi-function support.
- In scope:
  - Global logistics support
  - Program-wide operational support
  - Multi-site service contracts
- Out of scope:
  - Event/hospitality support
  - Narrow, transactional service tasks
- Evidence: GLSS2-style broad logistics contract labeled relevant.

### Lane C: Engineering / R&D Systems Work (Secondary)
- Objective: capture applied technical work with defined system outcomes.
- In scope:
  - Real-time analysis, system development, simulation
  - Advanced propulsion or analogous technical domains
- Out of scope:
  - Generic testing/certification without mission/system scope
- Evidence: engineering/system-development opportunity labeled relevant.

### Lane D: Custom Fabrication / Prototype Manufacturing (Selective)
- Objective: include fabrication opportunities that are SOW/drawing driven, not catalog resale.
- In scope:
  - Parts fabrication tied to drawings/SOW
  - Extended-range/prototype-style fabrication lots
- Out of scope:
  - NSN-only replenishment and brand/OEM replacement parts
- Evidence: Quickstrike parts fabrication was relevant; most other parts buys were not.

## 2) Labeled SAM opportunities and extrapolated evaluation strategy

### Label distribution from first 50
- Relevant: 9
- Not relevant: 41

### Relevant themes
- `construction/facilities`: 6
- `professional services`: 1
- `engineering/R&D`: 1
- `fabrication/manufacturing`: 1

### Not relevant themes (dominant noise)
- `commodity part`: 22
- `medical/clinical`: 8
- `equipment purchase`: 3
- `event/hospitality`: 2
- `OEM/brand-specific part`: 2

### Extrapolated evaluation strategy
1. Apply an early noise gate before full scoring.
   - If title/snippet has strong commodity signature (NSN, part-only assembly terms), assign heavy penalty or drop from top ranks.
2. Promote scoped work statements over item-line buys.
   - Boost when title/reason contains project-scope verbs: renovation, replacement, build-out, expansion, maintenance BPA.
3. Treat medical/clinical as default negative unless company profile explicitly includes healthcare.
4. Keep amendments in watch mode unless scope-bearing terms are present.
5. De-duplicate aggressively before top-N ranking.

## 3) Keyword intents per lane

### Lane A: Facilities Modernization and Sustainment
- Intent-positive terms:
  - `renovation`, `build-out`, `replacement`, `expansion`, `HVAC maintenance`, `roofing`, `facility`, `BPA`
- Intent-negative terms:
  - `NSN`, `assembly`, `valve`, `wrench`, `stud`, `roller`, `fixture`, `cable assembly`

### Lane B: Program Logistics and Mission Support Services
- Intent-positive terms:
  - `global logistics`, `operations support`, `mission support`, `program support`, `SOF`, `services II`
- Intent-negative terms:
  - `lodging`, `hotel`, `hospitality`, `conference`, `catering`

### Lane C: Engineering / R&D Systems Work
- Intent-positive terms:
  - `real-time data analysis`, `system development`, `simulation`, `advanced propulsion`, `technical`
- Intent-negative terms:
  - `preventative maintenance` (when paired with medical equipment context), `certification only`

### Lane D: Custom Fabrication / Prototype Manufacturing
- Intent-positive terms:
  - `parts fabrication`, `drawings`, `statement of work`, `set-aside` (when paired with fabrication scope)
- Intent-negative terms:
  - `OEM`, `brand name`, `replacement part`, `NSN`, `single line item`

## 4) Hard filters / preferences

### Hard excludes (immediate precision gain)
- Commodity line-item signatures:
  - title contains `NSN`
  - title begins with FSC-style prefix pattern like `^\d{2}--`
  - high-confidence part terms without scope terms (`assembly`, `stud`, `roller`, `valve`, `wrench`)
- Medical/clinical signatures unless profile override:
  - `pharmacy`, `radiopharmaceutical`, `dental`, `patient`, `lithotripsy`, `Omnicell`
- Event/hospitality signatures:
  - `lodging`, `hotel`, `hospitality`, `event` when no mission/service scope

### Soft preferences (do not hard-gate yet)
- NAICS preference from observed relevants:
  - `236220`, `238160`, `238220`, `236118`, `561990`, `541715`, `811310`, `332710`
- Scope-bearing opportunity types:
  - broad service and construction scopes, BPA/IDIQ-style recurring work

### Deprioritize (not hard drop)
- Amendment-only entries where title/snippet lacks work-scope language
- Duplicate opportunities already represented in higher-ranked entries

## 5) Priority rules for ranking contracts of interest

Use weighted scoring (0-100), then apply gates/penalties.

### Scoring dimensions
- Scope specificity and deliverable clarity: 30
- Lane intent match density: 20
- Contract breadth (program/project scale): 15
- Technical/mission fit: 15
- Actionability (response window, stage): 10
- Data quality (not duplicate, non-fragmentary): 10

### Rulebook
- `gate_commodity_signature`
  - Condition: `NSN` or `^\d{2}--` prefix + no scope-bearing terms
  - Action: gate or `-100`
- `penalty_medical_cluster`
  - Condition: medical/clinical lexicon hit and no explicit profile override
  - Action: `-35`
- `penalty_event_hospitality`
  - Condition: event/hospitality lexicon hit and no mission-service language
  - Action: `-25`
- `boost_facilities_scope_bundle`
  - Condition: at least two of `renovation/build-out/replacement/expansion/HVAC/roofing`
  - Action: `+20`
- `boost_broad_services_bundle`
  - Condition: logistics/program/mission support terms with contract-wide wording (`global`, `services II`, multi-site indicators)
  - Action: `+15`
- `boost_engineering_systems_bundle`
  - Condition: engineering+system-development terms co-occur
  - Action: `+15`
- `boost_fabrication_sow_bundle`
  - Condition: `fabrication` + (`drawings` or `SOW`)
  - Action: `+12`
- `penalty_low_context_amendment`
  - Condition: amendment wording with no scope-bearing terms
  - Action: `-12`
- `penalty_duplicate`
  - Condition: duplicate flag true
  - Action: `-8`

### Tie-breakers
1. More scoped-work terms in same lane beats isolated keyword hits.
2. Opportunities with deliverable-bearing text beat item-coded notices.
3. Non-duplicate entries beat duplicates at equal score.
4. Longer usable response window beats near-close entries at equal score.

## Operational files generated from this analysis
- `C:\Users\kabyl\OneDrive\Documents\Github\ShadowScope\data\sam_first50_labels_parsed.json`
- `C:\Users\kabyl\OneDrive\Documents\Github\ShadowScope\data\sam_first50_relevant.csv`
- `C:\Users\kabyl\OneDrive\Documents\Github\ShadowScope\data\sam_first50_not_relevant.csv`
- `C:\Users\kabyl\OneDrive\Documents\Github\ShadowScope\data\sam_first50_summary.json`
