"use client";

import { useEffect, useState } from "react";

export function useChartHeight() {
  const [height, setHeight] = useState(420);

  useEffect(() => {
    function handleResize() {
      if (window.innerWidth < 768) {
        setHeight(280);
        return;
      }

      if (window.innerWidth < 1280) {
        setHeight(340);
        return;
      }

      setHeight(420);
    }

    handleResize();
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  return height;
}
