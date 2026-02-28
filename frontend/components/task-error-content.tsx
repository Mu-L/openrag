"use client";

import { useMemo, useState } from "react";
import { ArrowDown, ArrowUp, ChevronDown, ChevronUp, XCircle } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { type Task } from "@/contexts/task-context";
import { formatTaskTimestamp, parseTimestamp } from "@/lib/time-utils";

interface TaskErrorContentProps {
  task: Task;
  mode?: "recent" | "past";
  nowMs?: number;
}

export function TaskErrorContent({
  task,
  mode = "recent",
  nowMs = Date.now(),
}: TaskErrorContentProps) {
  const [expanded, setExpanded] = useState(false);

  const failedEntries = useMemo(
    () =>
      Object.entries(task.files || {}).filter(
        ([, fileInfo]) =>
          fileInfo?.status === "failed" || fileInfo?.status === "error",
      ),
    [task.files],
  );

  const failedCount = task.failed_files ?? failedEntries.length;
  const successCount = task.successful_files ?? 0;
  const updatedAt = parseTimestamp(task.updated_at);

  if (failedCount <= 0 && failedEntries.length === 0) {
    return null;
  }

  return (
    <div className="flex flex-col gap-1 border-t border-muted w-full hover:bg-muted/60 transition-colors px-4 py-2">
      <div className="flex items-center justify-between gap-2 min-w-0">
        <div className="flex items-center gap-2 min-w-0">
          <XCircle className="h-5 w-5 text-destructive shrink-0" />
          <p className="text-mmd truncate">Task {task.task_id.slice(0, 8)}...</p>
        </div>
        {expanded ? (
          <Badge
            variant="outline"
            className="bg-destructive/10 text-destructive border-destructive/20"
          >
            Failed
          </Badge>
        ) : (
          <p className="text-xs text-destructive shrink-0">{failedCount} failed</p>
        )}
      </div>

      <div className="flex flex-col justify-between gap-1">
        <p className="text-xxs text-muted-foreground">
          {formatTaskTimestamp(updatedAt, mode, nowMs)}
        </p>

        <button
          type="button"
          onClick={() => setExpanded((prev) => !prev)}
          className="flex items-center gap-1 text-sm text-destructive hover:text-destructive/80 transition-colors"
        >
          <span className="text-muted-foreground text-xs">{successCount} success,</span>
          <span className="text-destructive text-xs">{failedCount} failed</span>
          {expanded ? (
            <ChevronDown className="h-4 w-4 text-destructive" />
          ) : (
            <ChevronUp className="h-4 w-4 text-destructive" />
          )}
        </button>
      </div>

      {expanded && (
        <div className="rounded-xl border border-destructive/20 bg-failure-soft p-3">
          <p className="text-xs font-medium text-failure-log mb-2 sticky top-0">
            Failure Log <span className="text-failure-muted">({failedCount} of {failedCount} pending)</span>
          </p>
          <div className="max-h-56 overflow-y-auto flex flex-col gap-2">
            {failedEntries.map(([filePath, fileInfo], index) => {
              const fileName =
                fileInfo.filename || filePath.split("/").pop() || filePath;
              const message =
                typeof fileInfo.error === "string" && fileInfo.error.trim()
                  ? fileInfo.error.trim()
                  : task.error || "Unknown error";

              return (
                <div key={`${task.task_id}-${filePath}-${index}`} className="space-y-1">
                  <p className="text-xs text-failure-file truncate">
                    {">"} {fileName}
                  </p>
                  <p className="text-xs text-failure-message break-words">{message}</p>
                </div>
              );
            })}
          </div>
          <div className="mt-2 text-[9px] text-failure-scroll/40 flex items-center justify-center gap-1">
            <div className="flex items-center gap-0">
            <ArrowUp className="h-2 w-2" />
            <ArrowDown className="h-2 w-2" />
            </div>
            <span>scroll · {failedCount} errors</span>
          </div>
        </div>
      )}
    </div>
  );
}

