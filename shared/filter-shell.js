(function attachFilterShell(globalScope) {
  function setLayerOpen(toggle, layer, open) {
    if (!layer) {
      return;
    }

    layer.hidden = !open;
    layer.classList.toggle("open", open);
    layer.setAttribute("aria-hidden", String(!open));

    if (toggle) {
      toggle.setAttribute("aria-expanded", String(open));
      toggle.classList.toggle("active", open);
    }
  }

  function bindExclusiveLayers({ shell, items }) {
    const normalizedItems = (items || []).filter((item) => item && item.layer);

    function closeAll(exceptToggle = null) {
      normalizedItems.forEach((item) => {
        const keepOpen = exceptToggle && item.toggle === exceptToggle;
        setLayerOpen(item.toggle || null, item.layer, keepOpen);
      });
    }

    normalizedItems.forEach((item) => {
      if (!item.toggle) {
        return;
      }

      item.toggle.addEventListener("click", () => {
        const isOpen = item.layer.classList.contains("open");
        closeAll(isOpen ? null : item.toggle);
      });
    });

    document.addEventListener("click", (event) => {
      if (shell && shell.contains(event.target)) {
        return;
      }

      closeAll(null);
    });

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        closeAll(null);
      }
    });

    return {
      closeAll,
      setLayerOpen,
    };
  }

  const api = {
    bindExclusiveLayers,
    setLayerOpen,
  };

  globalScope.CommodityWatchFilterShell = api;
})(window);
