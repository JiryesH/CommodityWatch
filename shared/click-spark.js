(function attachClickSpark(globalScope) {
  const DEFAULTS = Object.freeze({
    sparkSize: 10,
    sparkRadius: 15,
    sparkCount: 8,
    duration: 380,
    extraScale: 1,
    lineWidth: 2,
  });

  const EDITABLE_TARGET_SELECTOR = [
    "textarea",
    "select",
    '[contenteditable=""]',
    '[contenteditable="true"]',
    'input:not([type="button"]):not([type="checkbox"]):not([type="color"]):not([type="file"]):not([type="image"]):not([type="radio"]):not([type="range"]):not([type="reset"]):not([type="submit"])',
  ].join(",");

  function easeOut(value) {
    return value * (2 - value);
  }

  function readSparkColor() {
    const rootStyles = globalScope.getComputedStyle(document.documentElement);
    const bodyStyles = document.body ? globalScope.getComputedStyle(document.body) : null;

    return (
      (bodyStyles ? bodyStyles.getPropertyValue("--click-spark-color").trim() : "") ||
      rootStyles.getPropertyValue("--click-spark-color").trim() ||
      rootStyles.getPropertyValue("--color-amber").trim() ||
      "#e8a020"
    );
  }

  function shouldIgnoreTarget(target) {
    if (!(target instanceof Element)) {
      return false;
    }

    return Boolean(target.closest('[data-click-spark="off"]')) || Boolean(target.closest(EDITABLE_TARGET_SELECTOR));
  }

  function createCanvas() {
    const canvas = document.createElement("canvas");
    canvas.setAttribute("aria-hidden", "true");
    canvas.setAttribute("data-click-spark-canvas", "");
    Object.assign(canvas.style, {
      position: "fixed",
      inset: "0",
      width: "100vw",
      height: "100vh",
      display: "block",
      pointerEvents: "none",
      userSelect: "none",
      zIndex: "9999",
    });
    return canvas;
  }

  function createController(options = {}) {
    if (typeof document === "undefined" || typeof globalScope === "undefined") {
      return null;
    }

    if (typeof globalScope.matchMedia === "function" && globalScope.matchMedia("(prefers-reduced-motion: reduce)").matches) {
      return null;
    }

    const canvas = createCanvas();
    document.body.appendChild(canvas);

    const context = canvas.getContext("2d");
    if (!context) {
      canvas.remove();
      return null;
    }

    const settings = {
      ...DEFAULTS,
      ...options,
      sparkColor: options.sparkColor || readSparkColor(),
    };

    let devicePixelRatio = 1;
    let viewportWidth = 0;
    let viewportHeight = 0;
    let animationFrameId = 0;
    let sparks = [];

    function resizeCanvas() {
      viewportWidth = globalScope.innerWidth;
      viewportHeight = globalScope.innerHeight;
      devicePixelRatio = Math.max(globalScope.devicePixelRatio || 1, 1);
      canvas.width = Math.round(viewportWidth * devicePixelRatio);
      canvas.height = Math.round(viewportHeight * devicePixelRatio);
      canvas.style.width = `${viewportWidth}px`;
      canvas.style.height = `${viewportHeight}px`;
    }

    function clearCanvas() {
      context.setTransform(1, 0, 0, 1, 0, 0);
      context.clearRect(0, 0, canvas.width, canvas.height);
    }

    function drawSpark(spark, timestamp) {
      const elapsed = timestamp - spark.startTime;
      if (elapsed >= settings.duration) {
        return false;
      }

      const progress = elapsed / settings.duration;
      const eased = easeOut(progress);
      const distance = eased * settings.sparkRadius * settings.extraScale;
      const lineLength = settings.sparkSize * (1 - eased);
      const x1 = spark.x + distance * Math.cos(spark.angle);
      const y1 = spark.y + distance * Math.sin(spark.angle);
      const x2 = spark.x + (distance + lineLength) * Math.cos(spark.angle);
      const y2 = spark.y + (distance + lineLength) * Math.sin(spark.angle);

      context.beginPath();
      context.moveTo(x1, y1);
      context.lineTo(x2, y2);
      context.stroke();

      return true;
    }

    function render(timestamp) {
      clearCanvas();
      context.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
      context.strokeStyle = settings.sparkColor;
      context.lineWidth = settings.lineWidth;
      context.lineCap = "round";

      sparks = sparks.filter((spark) => drawSpark(spark, timestamp));

      if (sparks.length > 0) {
        animationFrameId = globalScope.requestAnimationFrame(render);
        return;
      }

      animationFrameId = 0;
      clearCanvas();
    }

    function requestRender() {
      if (!animationFrameId) {
        animationFrameId = globalScope.requestAnimationFrame(render);
      }
    }

    function spawnSparks(clientX, clientY) {
      const now = globalScope.performance.now();
      const newSparks = Array.from({ length: settings.sparkCount }, (_, index) => ({
        x: clientX,
        y: clientY,
        angle: (2 * Math.PI * index) / settings.sparkCount,
        startTime: now,
      }));

      sparks.push(...newSparks);
      requestRender();
    }

    function handlePointerDown(event) {
      if (!event.isPrimary || event.button !== 0 || shouldIgnoreTarget(event.target)) {
        return;
      }

      spawnSparks(event.clientX, event.clientY);
    }

    resizeCanvas();

    const handleResize = () => {
      resizeCanvas();
    };

    document.addEventListener("pointerdown", handlePointerDown, { passive: true });
    globalScope.addEventListener("resize", handleResize, { passive: true });

    return {
      destroy() {
        if (animationFrameId) {
          globalScope.cancelAnimationFrame(animationFrameId);
          animationFrameId = 0;
        }

        sparks = [];
        document.removeEventListener("pointerdown", handlePointerDown, { passive: true });
        globalScope.removeEventListener("resize", handleResize, { passive: true });
        canvas.remove();
      },
    };
  }

  function initialize() {
    if (
      document.documentElement.dataset.clickSpark === "off" ||
      (document.body && document.body.dataset.clickSpark === "off")
    ) {
      return;
    }

    var existingController = globalScope.CommodityWatchClickSparkController;
    if (existingController) {
      return;
    }

    var controller = createController();
    globalScope.CommodityWatchClickSparkController = controller;
  }

  var api = {
    createController,
  };

  globalScope.CommodityWatchClickSpark = api;

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initialize, { once: true });
  } else {
    initialize();
  }
})(window);
