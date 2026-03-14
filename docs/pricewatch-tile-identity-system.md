# PriceWatch Tile Identity System

This proposal replaces illustration-led front faces for non-Energy and non-Metals PriceWatch tiles with a durable artifact system. The tile remains the same object structurally. What changes is the square face inside the card: it now behaves like a trade-world surface built from type, pattern, framing, substrate, and print detail.

The benchmark is immediate sector recognition without relying on drawings. A user should be able to glance at a row of tiles and understand which one belongs to grain, fertilizer, seafood, or lumber before reading the full label.

## Phase 1. System Proposal

### Sector rationale

#### Agriculture

Agriculture should feel archival, exported, and classified. The reference is a commodity certificate or produce carton face rather than a plant illustration. The warmth comes from paper stock, grading language, harvest registers, and mercantile coding.

#### Fertilizers and agricultural chemicals

Fertilizers should feel technical, industrial, and traded in bulk. The reference is an assay sheet merged with industrial sack print. It needs technical credibility without reading as a classroom chemistry diagram.

#### Livestock, dairy, and seafood

This sector should feel inspected, graded, packed, and shipped. The reference is a meat auction lot card, seafood carton mark, or cold-chain label. The tone is wholesale and commercial rather than culinary or consumer-facing.

#### Forest and wood products

Forest should feel like mill output. The reference is a lumber grade card or timber certification stamp. This sector is the simplest and most materially direct, so the visual identity should be the strongest and least ornamented.

### Substrate treatment by sector

#### Agriculture substrate

- Base feel: warm card stock, parchment, sack paper, export carton board
- Finish: matte, slightly aged, softly sunlit
- Palette behavior: derived from commodity family but held in muted, printed tones
- Surface logic: subtle tonal wash, visible frame, no glossy effects

#### Fertilizer substrate

- Base feel: technical paper, bulk bag print, warehouse label stock
- Finish: cool white to pale grey with utility ink
- Palette behavior: restrained, mostly white-blue-grey with commodity-specific emphasis via banding and stencil contrast
- Surface logic: grid or ledger base, heavier notation bands, more explicit inspection structure

#### Livestock / seafood substrate

- Base feel: grading card, inspection ticket, carton label, cold-chain stencil
- Finish: dense and inked, with strong boxed framing
- Palette behavior: deeper, richer fields with pale utility ink
- Surface logic: heavier edges, prominent stamps, more assertive lot-card hierarchy

#### Forest substrate

- Base feel: kiln-dried board stamp, mill certificate, lumber-grade card
- Finish: dry, warm, materially grounded
- Palette behavior: amber, oak, and brown with restrained contrast
- Surface logic: central emblem plus strong board-mark framing

### Pattern language

The system uses six pattern families. Each family scales across multiple commodities while preserving recognition.

#### 1. Grain pattern family

- Applies to: wheat, corn, barley, rice
- Motif: field rows, storage-bay divisions, harvest bands, ruled certificate stripes
- Density rule: open spacing for rice, denser bands for wheat and barley, stronger vertical rhythm for corn
- Read: archival crop board

#### 2. Oilseed pattern family

- Applies to: soybeans, soybean oil, soybean meal, palm oil, palm kernel oil, coconut oil, rapeseed oil, sunflower oil, groundnuts
- Motif: seed matrices, press rings, process circles, extraction register lines
- Density rule: raw seed commodities use wider dot spacing; oils use stronger concentric process geometry; meals use denser micro-grid or particulate logic
- Read: crush plant and export tank economy

#### 3. Tropical softs pattern family

- Applies to: sugar, coffee, cocoa, cotton, bananas, orange, rubber
- Motif: crate grids, woven sack lines, carton stamp boxes, port-mark stripes
- Density rule: cotton gets the cleanest surface, rubber the most industrial grid, coffee/cocoa the most overprint-heavy
- Read: mercantile export artifact

#### 4. Fertilizer assay pattern family

- Applies to: phosphate rock, DAP, TSP, urea, potassium chloride
- Motif: graph ledger, assay boxes, stencil bars, bulk-bag print panels, granule matrices
- Density rule: phosphate rock stays raw and blocky; DAP/TSP share cousin layouts with distinct notation bands; urea is the cleanest; KCl is the most sack-stamped
- Read: industrial chemical lot control

#### 5. Livestock / seafood packing pattern family

- Applies to: beef, poultry, lamb, swine, shrimp, salmon, fish meal
- Motif: inspection blocks, grade bars, cold-chain meshes, transport stripes, boxed lot references
- Density rule: meats use heavier framing and vertical grade bars; seafood uses net logic and container marks; fish meal leans feed-bag and warehouse coding
- Read: market-grade packaging artifact

#### 6. Forest mill pattern family

- Applies to: lumber
- Motif: saw-line rhythm, board marks, concentric emblem rings, mill stamp hierarchy
- Density rule: restrained, large-shape first
- Read: certified timber surface

### Typography rules

#### Hero code

- Role: the hero code is the main visual object on the tile face
- Typeface: serif for Agriculture, Livestock, Seafood, and Forest; mono for Fertilizers
- Scale: 55-85 px equivalent within current tile dimensions
- Color: high-contrast but softened slightly to feel printed, not digital
- Placement: upper-middle to center, leaving clear space for descriptor and metadata bands
- Style: one code only, never paired with icons or drawings

#### Descriptor line

- Role: gives semantic market specificity
- Typeface: monospace, uppercase
- Scale: small but intentionally legible
- Placement: inside the artifact face near the lower register band
- Style: two-part descriptor separated by slash, always factual and trade-literate

#### Support labels

- Includes: serial number, lot reference, stamp label, grade marker, origin code
- Typeface: monospace uppercase
- Scale: secondary and tertiary, used sparingly
- Placement: corners, bottom edge, or stamp box
- Style: should feel administrative rather than ornamental

### Border, framing, and print-detail rules

- Every tile face gets an outer border and a secondary inner register frame.
- Every tile face gets at least one serial line near the top left quadrant.
- Every tile face gets four subtle register marks or corner crop-like indicators.
- Every tile face gets one stamp treatment at the lower right: rectangular for agriculture/fertilizer/forest, circular or pill-like for meat and seafood.
- The existing source and benchmark pills stay, but their fill and border inherit the sector surface.
- Hover shimmer remains shared across sectors; the sector system only changes the base face design.

### Code system for the full list

#### Agriculture

| Commodity | Code | Descriptor direction |
| --- | --- | --- |
| Wheat | `WHT` | `GLOBAL / US SRW` |
| Corn | `CRN` | `US NO.2 / YELLOW` |
| Barley | `BRLY` | `FEED / MALTING` |
| Rice | `RICE` | `THAI 5% / VIETNAM` |
| Soybeans | `SOY` | `BEAN / CRUSH` |
| Soybean oil | `SBO` | `CRUDE / DEGUMMED` |
| Soybean meal | `SBM` | `48% PRO / FEED` |
| Palm oil | `PO` | `CRUDE / RBD` |
| Palm kernel oil | `PKO` | `KERNEL / REFINED` |
| Coconut oil | `COCO` | `COPRA / RBD` |
| Rapeseed oil | `RO` | `CANOLA / FOB` |
| Sunflower oil | `SFO` | `BLACK SEA / REFINED` |
| Groundnuts | `GN` | `SHELLED / EXPORT` |
| Sugar | `SGR` | `RAW / #11` |
| Coffee | `COFF` | `ARABICA / ROBUSTA` |
| Cocoa | `CCOA` | `ICE / ICCO` |
| Cotton | `CTTN` | `A INDEX / MIDDLING` |
| Bananas | `BANA` | `EXPORT / CAVENDISH` |
| Orange | `ORNG` | `JUICE / FCOJ` |
| Rubber | `RUBR` | `RSS3 / TSR20` |

#### Fertilizers and agricultural chemicals

| Commodity | Code | Descriptor direction |
| --- | --- | --- |
| Phosphate rock | `ROCK` | `BENEFICIATED / BPL` |
| DAP | `DAP` | `18-46-0 / BULK` |
| TSP | `TSP` | `0-46-0 / GRANULAR` |
| Urea | `UREA` | `N 46% / PRILLED` |
| Potassium chloride | `KCL` | `MOP / STANDARD` |

#### Livestock, dairy, and seafood

| Commodity | Code | Descriptor direction |
| --- | --- | --- |
| Beef | `BEEF` | `CARCASS / WHOLESALE` |
| Poultry | `PTRY` | `BROILER / WHOLE BIRD` |
| Lamb | `LAMB` | `CARCASS / TRADE` |
| Swine | `SWNE` | `LEAN HOG / CUTOUT` |
| Shrimp | `SHRP` | `FROZEN / SHELL-ON` |
| Salmon | `SLMN` | `ATLANTIC / FARMED` |
| Fish meal | `FML` | `65% PRO / FEED` |

#### Forest and wood products

| Commodity | Code | Descriptor direction |
| --- | --- | --- |
| Lumber | `LMBR` | `SPF / RANDOM LENGTH` |

### Variation rules

- Variation starts with family pattern, not a new one-off idea.
- Commodity differences come from code, descriptor, palette tuning, pattern density, and support-label wording.
- Within a family, layout is fixed and only the emphasis shifts. Example: soybeans and palm oil share the oilseed geometry, but soybeans favors crush-board wording while palm oil favors refinery/export wording.
- Sector drift is controlled by substrate and frame rules. A seafood tile should never borrow a fertilizer grid, and a fertilizer tile should never look archival and warm like wheat.
- The system should support new commodities by assigning them to a sector and family first, then adjusting code and descriptor text second.

## Phase 2. Pilot Tiles

The pilot set proves the system across all four unfinished sectors. A browser mock page is available at [sandbox/pricewatch-tile-pilots/index.html](/Users/jiryes/Desktop/Projects/Contango/sandbox/pricewatch-tile-pilots/index.html).

### Wheat

- Code: `WHT`
- Descriptor: `GLOBAL / US SRW`
- Pattern: grain rows and certificate striping
- Rationale: Wheat is the clearest expression of the archival grain language. The face reads like a grain-board certificate rather than a crop illustration. It establishes the warm agricultural substrate and the sector’s institutional tone.

### Rice

- Code: `RICE`
- Descriptor: `THAI 5% / VIETNAM`
- Pattern: wider, cleaner row structure than wheat
- Rationale: Rice stays inside the grain family but uses lighter spacing and more export-oriented wording. That makes it feel polished and globally traded rather than harvested and board-graded in the same way as wheat.

### Soybeans

- Code: `SOY`
- Descriptor: `BEAN / CRUSH`
- Pattern: seed matrix plus process rings
- Rationale: Soybeans needs to sit between raw agricultural output and processing economics. The ring geometry introduces crush logic, so it feels tied to meal/oil conversion rather than just another warm grain card.

### Palm oil

- Code: `PO`
- Descriptor: `CRUDE / RBD`
- Pattern: denser process rings and export-tank framing
- Rationale: Palm oil belongs in the oilseed family but should feel more processed, port-facing, and refinery-adjacent than soybeans. The shorter code and denser geometry create a more industrial commodity signal without leaving the Agriculture sector.

### Urea

- Code: `UREA`
- Descriptor: `N 46% / PRILLED`
- Pattern: clean assay grid and notation band
- Rationale: Urea should be the cleanest fertilizer artifact. The mono hero code, cool paper substrate, and assay structure make it feel technical and traded in bulk, not diagrammatic or educational.

### Potassium chloride

- Code: `KCL`
- Descriptor: `MOP / STANDARD`
- Pattern: assay grid with stronger sack-stamp emphasis
- Rationale: KCl should feel more warehouse-stenciled than urea. It shares the fertilizer system but skews toward bag-print and bulk handling language, giving it a more rugged industrial character.

### Beef

- Code: `BEEF`
- Descriptor: `CARCASS / WHOLESALE`
- Pattern: heavier framing with lot-card stripe structure
- Rationale: Beef sets the wholesale meat language. The dark field, pale ink, and rounded inspection stamp move the tile away from illustration and into grading-card territory.

### Shrimp

- Code: `SHRP`
- Descriptor: `FROZEN / SHELL-ON`
- Pattern: cold-chain net geometry
- Rationale: Shrimp should feel boxed, chilled, and packed for export. The pattern is lighter and more meshed than the meat cards, so the seafood branch has its own logistics vocabulary.

### Salmon

- Code: `SLMN`
- Descriptor: `ATLANTIC / FARMED`
- Pattern: cold-chain mesh with more linear crate-mark balance
- Rationale: Salmon stays in the seafood system but leans more toward carton mark and handling code than shrimp. That lets the two feel related but not interchangeable.

### Lumber

- Code: `LMBR`
- Descriptor: `SPF / RANDOM LENGTH`
- Pattern: board-mark rhythm plus ring emblem
- Rationale: Lumber should be the clearest, boldest, and least cluttered face in the unfinished sectors. The board-stamp framing and central emblem let it read as a material certification surface rather than a decorative wood motif.

## Phase 3. Rollout Set

The rest of the rollout follows by assigning each commodity to an existing family, then tuning the descriptor, palette, and support labels.

### Agriculture rollout

- Grains: wheat, corn, barley, rice
- Oilseeds and oils: soybeans, soybean oil, soybean meal, palm oil, palm kernel oil, coconut oil, rapeseed oil, sunflower oil, groundnuts
- Softs and tropicals: sugar, coffee, cocoa, cotton, bananas, orange, rubber

### Fertilizer rollout

- Raw mineral: phosphate rock
- Processed bulk fertilizers: DAP, TSP, urea, potassium chloride

### Livestock and seafood rollout

- Meat cards: beef, poultry, lamb, swine
- Seafood and feed cards: shrimp, salmon, fish meal

### Forest rollout

- Lumber remains a single-commodity identity and can carry the cleanest system expression.

## Special handling notes

### Palm kernel oil vs palm oil

These need to feel like close industrial cousins, not duplicates. They should share the same sector palette family but differ via descriptor wording, process density, and support labels tied to kernel versus crude/refined stream.

### Soybean oil vs soybean meal vs soybeans

This trio needs one shared origin system with explicit processing separation. The safest route is to keep the same code root and vary the artifact language:

- `SOY`: raw bean / crush
- `SBO`: refined / degummed / tank
- `SBM`: protein / feed / meal lot

### Sugar, coffee, cocoa, cotton, bananas, orange, rubber

These should all live inside the tropical softs family, but they need stronger descriptor specificity than the grains. Without that specificity, they risk looking like interchangeable warm export labels.

### Phosphate rock

This one should feel less processed than DAP/TSP/urea/KCl. It needs rougher block notation and more mineral-assay language so it reads as feedstock rather than finished input.

### Fish meal

Fish meal should not look like seafood retail. It belongs visually closer to industrial feed packaging than to shrimp or salmon carton marks. It sits in the seafood branch by origin, but its artifact vocabulary should borrow from warehouse feed sacks.

## Recommendation

Approve the artifact system and the 10 pilot fronts first, then replace the current non-Energy/non-Metals tile-face renderer with a production version of the same layout language. The sandbox pilot proves the system without committing the live app to a brittle illustration pipeline.
