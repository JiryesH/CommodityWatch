export function EmptyState({ title, message }: { title: string; message: string }) {
  return (
    <div className="card-surface p-5">
      <h2 className="text-h3 text-foreground">{title}</h2>
      <p className="mt-2 max-w-xl text-body text-foreground-secondary">{message}</p>
    </div>
  );
}
