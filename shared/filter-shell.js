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
    const toggleHandlers = new Map();
    const previousBinding = shell?.__commodityWatchFilterShellBinding || null;

    previousBinding?.destroy?.();

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

      const handleToggleClick = () => {
        const isOpen = item.layer.classList.contains("open");
        closeAll(isOpen ? null : item.toggle);
      };

      toggleHandlers.set(item.toggle, handleToggleClick);
      item.toggle.addEventListener("click", handleToggleClick);
    });

    const handleDocumentClick = (event) => {
      if (shell && shell.contains(event.target)) {
        return;
      }

      closeAll(null);
    };

    const handleDocumentKeydown = (event) => {
      if (event.key === "Escape") {
        closeAll(null);
      }
    };

    document.addEventListener("click", handleDocumentClick);
    document.addEventListener("keydown", handleDocumentKeydown);

    const api = {
      closeAll,
      setLayerOpen,
      destroy() {
        toggleHandlers.forEach((handleToggleClick, toggle) => {
          toggle.removeEventListener("click", handleToggleClick);
        });
        document.removeEventListener("click", handleDocumentClick);
        document.removeEventListener("keydown", handleDocumentKeydown);
        if (shell && shell.__commodityWatchFilterShellBinding === api) {
          delete shell.__commodityWatchFilterShellBinding;
        }
      },
    };

    if (shell) {
      shell.__commodityWatchFilterShellBinding = api;
    }

    return api;
  }

  const api = {
    bindExclusiveLayers,
    setLayerOpen,
  };

  globalScope.CommodityWatchFilterShell = api;
})(window);
