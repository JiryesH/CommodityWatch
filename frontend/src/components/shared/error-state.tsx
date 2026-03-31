"use client";

import { Button } from "@/components/ui/button";

interface ErrorStateProps {
  title?: string;
  message: string;
  onRetry?: () => void;
}

export function ErrorState({ title = "Unable to load data", message, onRetry }: ErrorStateProps) {
  return (
    <div className="card-surface flex flex-col gap-3 p-4">
      <div>
        <h2 className="text-h3 text-foreground">{title}</h2>
        <p className="mt-2 text-body text-foreground-secondary">{message}</p>
      </div>
      {onRetry ? (
        <div>
          <Button onClick={onRetry} variant="subtle">
            Unable to load data. Tap to retry.
          </Button>
        </div>
      ) : null}
    </div>
  );
}
