"use client";

import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  type ColumnDef,
  type SortingState,
  useReactTable,
} from "@tanstack/react-table";
import { ArrowDown, ArrowUp, ArrowUpDown, Download } from "lucide-react";
import { useMemo, useState } from "react";

import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils/cn";

interface DataTableProps<TData> {
  columns: ColumnDef<TData>[];
  data: TData[];
  exportName?: string;
  compact?: boolean;
  title?: string;
  description?: string;
}

function csvEscape(value: unknown) {
  const stringValue = String(value ?? "");
  if (/[,"\n]/.test(stringValue)) {
    return `"${stringValue.replace(/"/g, '""')}"`;
  }
  return stringValue;
}

export function DataTable<TData>({
  columns,
  data,
  exportName = "inventory-data",
  compact = false,
  title = "Recent releases",
  description = "Sortable table of the latest 20 releases.",
}: DataTableProps<TData>) {
  const [sorting, setSorting] = useState<SortingState>([]);

  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    onSortingChange: setSorting,
    state: { sorting },
  });

  const csv = useMemo(() => {
    const headers = table
      .getVisibleFlatColumns()
      .filter((column) => column.id !== "actions")
      .map((column) => column.columnDef.header?.toString() ?? column.id);
    const rows = table.getRowModel().rows.map((row) =>
      row
        .getVisibleCells()
        .filter((cell) => cell.column.id !== "actions")
        .map((cell) => csvEscape(cell.getValue()))
        .join(","),
    );

    return [headers.map(csvEscape).join(","), ...rows].join("\n");
  }, [table]);

  function handleDownload() {
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `${exportName}.csv`;
    link.click();
    URL.revokeObjectURL(url);
  }

  return (
    <div className="card-surface overflow-hidden">
      <div className="flex items-center justify-between gap-3 border-b border-border-subtle px-4 py-3">
        <div>
          <h2 className="text-h3 text-foreground">{title}</h2>
          <p className="mt-1 text-caption text-foreground-muted">{description}</p>
        </div>
        <Button onClick={handleDownload} size="sm" variant="subtle">
          <Download className="h-4 w-4" />
          Download CSV
        </Button>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full border-collapse">
          <thead className="bg-surface-alt">
            {table.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id}>
                {headerGroup.headers.map((header) => (
                  <th
                    className="border-b border-border-subtle px-3 py-2 text-left font-mono text-[11px] uppercase tracking-[0.08em] text-foreground-soft"
                    key={header.id}
                  >
                    {header.isPlaceholder ? null : (
                      <button
                        className="inline-flex items-center gap-1"
                        onClick={header.column.getToggleSortingHandler()}
                        type="button"
                      >
                        {flexRender(header.column.columnDef.header, header.getContext())}
                        {header.column.getIsSorted() === "asc" ? (
                          <ArrowUp className="h-3 w-3" />
                        ) : header.column.getIsSorted() === "desc" ? (
                          <ArrowDown className="h-3 w-3" />
                        ) : (
                          <ArrowUpDown className="h-3 w-3" />
                        )}
                      </button>
                    )}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map((row) => (
              <tr className="border-b border-border-subtle last:border-b-0" key={row.id}>
                {row.getVisibleCells().map((cell) => (
                  <td
                    className={cn(
                      "px-3 text-body text-foreground-secondary",
                      compact ? "py-2" : "py-3",
                      cell.column.id !== "date" ? "font-mono text-[13px]" : "",
                    )}
                    key={cell.id}
                  >
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
