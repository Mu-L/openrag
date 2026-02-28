import { type Task } from "@/contexts/task-context";
import { Bell, X } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { Button } from "@/components/ui/button";
import { TaskErrorContent } from "@/components/task-error-content";
import { useKnowledgeFilter } from "@/contexts/knowledge-filter-context";
import { parseTimestampMs } from "@/lib/time-utils";
import { TaskCollapsibleSection } from "@/components/task-collapsible-section";

interface FailedTasksInfoProps {
  failedTasks: Task[];
}

export const FailedTasksInfo = ({ failedTasks }: FailedTasksInfoProps) => {
  const [openSections, setOpenSections] = useState<Record<"recent" | "past", boolean>>({
    recent: true,
    past: false,
  });
  const [nowMs, setNowMs] = useState(() => Date.now());
  const { closePanelOnly } = useKnowledgeFilter();

  useEffect(() => {
    const id = window.setInterval(() => {
      setNowMs(Date.now());
    }, 1000);
    return () => {
      window.clearInterval(id);
    };
  }, []);

  const { recentTasks, pastTasks } = useMemo(() => {
    const fiveMinutesMs = 5 * 60 * 1000;

    const recent: Task[] = [];
    const past: Task[] = [];

    failedTasks.forEach((task) => {
      const updatedAtMs = parseTimestampMs(task.updated_at);
      if (updatedAtMs === null) {
        past.push(task);
        return;
      }

      if (nowMs - updatedAtMs < fiveMinutesMs) {
        recent.push(task);
      } else {
        past.push(task);
      }
    });

    return { recentTasks: recent, pastTasks: past };
  }, [failedTasks, nowMs]);

  const sections = useMemo(
    () => [
      {
        sectionKey: "recent" as const,
        title: "Recent Tasks",
        tasks: recentTasks,
        emptyText: "No recent failed tasks.",
        mode: "recent" as const,
      },
      {
        sectionKey: "past" as const,
        title: "Past Tasks",
        tasks: pastTasks,
        emptyText: "No past failed tasks.",
        mode: "past" as const,
      },
    ],
    [recentTasks, pastTasks],
  );

  return (
    <div className="h-full bg-background border-l overflow-y-auto">
      <div className="p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Bell className="h-5 w-5 text-muted-foreground" />
            <h3 className="font-semibold">Tasks</h3>
          </div>
          <Button
            variant="ghost"
            size="sm"
            className="h-8 w-8 p-0"
            onClick={closePanelOnly}
            aria-label="Close task panel"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>
      </div>

      {failedTasks.length === 0 ? (
        <div className="p-4 text-sm text-muted-foreground space-x-3">No failed tasks.</div>
      ) : (
        <div>
          {sections.map((section) => (
            <TaskCollapsibleSection
              key={section.sectionKey}
              title={section.title}
              items={section.tasks}
              isOpen={openSections[section.sectionKey]}
              onToggle={() =>
                setOpenSections((prev) => ({
                  ...prev,
                  [section.sectionKey]: !prev[section.sectionKey],
                }))
              }
              emptyText={section.emptyText}
              renderItem={(task) => (
                <TaskErrorContent
                  key={`${section.sectionKey}-${task.task_id}`}
                  task={task}
                  mode={section.mode}
                  nowMs={nowMs}
                />
              )}
            />
          ))}
        </div>
      )}
    </div>
  );
};

export default FailedTasksInfo;