export class CommodityFilterSelection {
  constructor(taxonomy) {
    this.allSectorIds = taxonomy.map((sector) => sector.id);
    this.subsectorIdsBySector = new Map(
      taxonomy.map((sector) => [sector.id, sector.subsectors.map((subsector) => subsector.id)])
    );

    this.reset();
  }

  reset() {
    this.selectedSectors = new Set(this.allSectorIds);
    this.selectedSubsectorsBySector = new Map();
  }

  getAllSectorIds() {
    return [...this.allSectorIds];
  }

  getSubsectorIdsForSector(sectorId) {
    return this.subsectorIdsBySector.get(sectorId) || [];
  }

  getVisibleSubsectorIds() {
    return this.allSectorIds.flatMap((sectorId) =>
      this.selectedSectors.has(sectorId) ? this.getSubsectorIdsForSector(sectorId) : []
    );
  }

  hasPartialSubsectorSelection() {
    return this.selectedSubsectorsBySector.size > 0;
  }

  isAllSelected() {
    return this.selectedSectors.size === this.allSectorIds.length && !this.hasPartialSubsectorSelection();
  }

  getSelectedSubsectorIdsForSector(sectorId) {
    if (!this.selectedSectors.has(sectorId)) {
      return [];
    }

    const explicitSelection = this.selectedSubsectorsBySector.get(sectorId);
    return explicitSelection ? [...explicitSelection] : this.getSubsectorIdsForSector(sectorId);
  }

  getSectorSelectionState(sectorId) {
    if (!this.selectedSectors.has(sectorId)) {
      return "none";
    }

    const totalSubsectorCount = this.getSubsectorIdsForSector(sectorId).length;
    const selectedSubsectorCount = this.getSelectedSubsectorIdsForSector(sectorId).length;

    if (selectedSubsectorCount === 0) {
      return "none";
    }

    return selectedSubsectorCount === totalSubsectorCount ? "full" : "partial";
  }

  isSubsectorSelected(sectorId, subsectorId) {
    if (!this.selectedSectors.has(sectorId)) {
      return false;
    }

    const explicitSelection = this.selectedSubsectorsBySector.get(sectorId);
    return explicitSelection
      ? explicitSelection.has(subsectorId)
      : this.getSubsectorIdsForSector(sectorId).includes(subsectorId);
  }

  clearSubsectorSelections() {
    this.selectedSubsectorsBySector.clear();
  }

  toggleSectorSelection(sectorId) {
    if (!sectorId || !this.subsectorIdsBySector.has(sectorId)) {
      return;
    }

    if (this.isAllSelected()) {
      this.selectedSectors = new Set([sectorId]);
      this.selectedSubsectorsBySector.clear();
      return;
    }

    if (this.selectedSectors.has(sectorId)) {
      this.selectedSectors.delete(sectorId);
      this.selectedSubsectorsBySector.delete(sectorId);

      if (!this.selectedSectors.size) {
        this.reset();
      }

      return;
    }

    this.selectedSectors.add(sectorId);
    this.selectedSubsectorsBySector.delete(sectorId);
  }

  toggleSubsectorSelection(sectorId, subsectorId) {
    if (!sectorId || !subsectorId || !this.selectedSectors.has(sectorId)) {
      return;
    }

    const allSubsectorIds = this.getSubsectorIdsForSector(sectorId);
    if (!allSubsectorIds.includes(subsectorId)) {
      return;
    }

    const nextSelection = new Set(this.selectedSubsectorsBySector.get(sectorId) || allSubsectorIds);

    if (nextSelection.has(subsectorId)) {
      nextSelection.delete(subsectorId);
    } else {
      nextSelection.add(subsectorId);
    }

    if (nextSelection.size === allSubsectorIds.length) {
      this.selectedSubsectorsBySector.delete(sectorId);
      return;
    }

    if (!nextSelection.size) {
      this.selectedSectors.delete(sectorId);
      this.selectedSubsectorsBySector.delete(sectorId);

      if (!this.selectedSectors.size) {
        this.reset();
      }

      return;
    }

    this.selectedSubsectorsBySector.set(sectorId, nextSelection);
  }
}
