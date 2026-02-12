/**
 * OpenRAG Settings MCP App - reuses frontend shadcn/ui components.
 * Uses native <select> for provider/model dropdowns so they work in MCP host iframes.
 */
import { useApp } from "@modelcontextprotocol/ext-apps/react";
import { StrictMode, useCallback, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";

import "../globals.css";

const LLM_PROVIDERS = ["openai", "anthropic", "ollama", "watsonx"] as const;
const EMBEDDING_PROVIDERS = ["openai", "ollama", "watsonx"] as const;

/** Get tool result text from either content[] or structuredContent.result[] (host-dependent). */
function getToolResultText(
  result: {
    content?: Array<{ type: string; text?: string }>;
    structuredContent?: { result?: Array<{ type: string; text?: string }> };
  }
): string | undefined {
  const fromContent = result.content?.find((c) => c.type === "text")?.text;
  if (fromContent) return fromContent;
  return result.structuredContent?.result?.find((c) => c.type === "text")?.text;
}

interface ModelOption {
  value: string;
  label?: string;
  default?: boolean;
}

const MAX_SYSTEM_PROMPT_CHARS = 4000;

interface AgentSettings {
  llm_provider: string;
  llm_model: string;
  system_prompt: string;
}

interface KnowledgeSettings {
  embedding_provider: string;
  embedding_model: string;
  chunk_size: number;
  chunk_overlap: number;
  table_structure: boolean;
  ocr: boolean;
  picture_descriptions: boolean;
}

const DEFAULT_AGENT: AgentSettings = {
  llm_provider: "",
  llm_model: "",
  system_prompt: "",
};

const DEFAULT_KNOWLEDGE: KnowledgeSettings = {
  embedding_provider: "",
  embedding_model: "",
  chunk_size: 1000,
  chunk_overlap: 200,
  table_structure: false,
  ocr: false,
  picture_descriptions: false,
};

function SettingsApp() {
  const [agent, setAgent] = useState<AgentSettings>(DEFAULT_AGENT);
  const [knowledge, setKnowledge] = useState<KnowledgeSettings>(DEFAULT_KNOWLEDGE);
  const [saving, setSaving] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [llmModels, setLlmModels] = useState<ModelOption[]>([]);
  const [embeddingModels, setEmbeddingModels] = useState<ModelOption[]>([]);
  const [loadingLlmModels, setLoadingLlmModels] = useState(false);
  const [loadingEmbeddingModels, setLoadingEmbeddingModels] = useState(false);

  const { app, error: appError } = useApp({
    appInfo: { name: "OpenRAG Settings", version: "1.0.0" },
    capabilities: {},
    onAppCreated: (app) => {
      app.ontoolresult = async (result) => {
        const textContent = getToolResultText(result);
        if (!textContent) return;
        try {
          const data = JSON.parse(textContent);
          if (data.agent) setAgent((prev) => ({ ...prev, ...data.agent }));
          if (data.knowledge) setKnowledge((prev) => ({ ...prev, ...data.knowledge }));
          setError(null);
        } catch {
          setError("Failed to parse settings");
        }
      };
    },
  });

  const refresh = useCallback(async () => {
    if (!app) return;
    setRefreshing(true);
    setError(null);
    try {
      const result = await app.callServerTool({
        name: "openrag_get_settings",
        arguments: {},
      });
      const textContent = getToolResultText(result);
      if (textContent) {
        const data = JSON.parse(textContent);
        if (data.agent) setAgent((prev) => ({ ...prev, ...data.agent }));
        if (data.knowledge) setKnowledge((prev) => ({ ...prev, ...data.knowledge }));
      }
    } catch (e) {
      setError(String(e));
    } finally {
      setRefreshing(false);
    }
  }, [app]);

  const save = useCallback(async () => {
    if (!app) return;
    setSaving(true);
    setError(null);
    try {
      const result = await app.callServerTool({
        name: "openrag_update_settings",
        arguments: {
          llm_provider: agent.llm_provider || undefined,
          llm_model: agent.llm_model || undefined,
          embedding_provider: knowledge.embedding_provider || undefined,
          embedding_model: knowledge.embedding_model || undefined,
          chunk_size: knowledge.chunk_size,
          chunk_overlap: knowledge.chunk_overlap,
          system_prompt: agent.system_prompt || undefined,
          table_structure: knowledge.table_structure,
          ocr: knowledge.ocr,
          picture_descriptions: knowledge.picture_descriptions,
        },
      });
      const textContent = getToolResultText(result);
      if (textContent) {
        const data = JSON.parse(textContent);
        if (data.error) setError(data.error);
      }
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  }, [app, agent, knowledge]);

  function getLlmModelPlaceholder(): string {
    if (loadingLlmModels) return "Loading models...";
    if (agent.llm_provider) return "No models or configure provider in OpenRAG Settings";
    return "Select provider first";
  }

  function getEmbeddingModelPlaceholder(): string {
    if (loadingEmbeddingModels) return "Loading models...";
    if (knowledge.embedding_provider) return "No models or configure provider in OpenRAG Settings";
    return "Select provider first";
  }

  const loadLlmModels = useCallback(
    async (provider: string) => {
      if (!app || !provider) {
        setLlmModels([]);
        return;
      }
      setLoadingLlmModels(true);
      try {
        const result = await app.callServerTool({
          name: "openrag_list_models",
          arguments: { provider },
        });
        const textContent = getToolResultText(result);
        if (textContent) {
          const parsed = JSON.parse(textContent);
          if (!parsed.error && parsed.language_models) {
            setLlmModels(parsed.language_models);
          } else {
            setLlmModels([]);
          }
        } else {
          setLlmModels([]);
        }
      } catch {
        setLlmModels([]);
      } finally {
        setLoadingLlmModels(false);
      }
    },
    [app]
  );

  const loadEmbeddingModels = useCallback(
    async (provider: string) => {
      if (!app || !provider) {
        setEmbeddingModels([]);
        return;
      }
      setLoadingEmbeddingModels(true);
      try {
        const result = await app.callServerTool({
          name: "openrag_list_models",
          arguments: { provider },
        });
        const textContent = getToolResultText(result);
        if (textContent) {
          const parsed = JSON.parse(textContent);
          if (!parsed.error && parsed.embedding_models) {
            setEmbeddingModels(parsed.embedding_models);
          } else {
            setEmbeddingModels([]);
          }
        } else {
          setEmbeddingModels([]);
        }
      } catch {
        setEmbeddingModels([]);
      } finally {
        setLoadingEmbeddingModels(false);
      }
    },
    [app]
  );

  useEffect(() => {
    if (app) refresh();
  }, [app]);

  useEffect(() => {
    if (app && agent.llm_provider) loadLlmModels(agent.llm_provider);
    else setLlmModels([]);
  }, [app, agent.llm_provider, loadLlmModels]);

  useEffect(() => {
    if (app && knowledge.embedding_provider) loadEmbeddingModels(knowledge.embedding_provider);
    else setEmbeddingModels([]);
  }, [app, knowledge.embedding_provider, loadEmbeddingModels]);

  if (appError) return <div className="text-destructive p-4">Error: {appError.message}</div>;
  if (!app) return <div className="p-4 text-muted-foreground">Connecting...</div>;

  return (
    <div className="space-y-6">
      {error && (
        <div className="rounded-lg border border-destructive/50 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          {error}
        </div>
      )}
      <div className="flex gap-2">
        <Button variant="outline" size="sm" onClick={refresh} disabled={refreshing}>
          {refreshing ? "Refreshing..." : "Refresh"}
        </Button>
        <Button size="sm" onClick={save} disabled={saving}>
          {saving ? "Saving..." : "Save settings"}
        </Button>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-lg">Agent</CardTitle>
          <CardDescription>
            Language model used for chat. Configure the provider in OpenRAG Settings (API keys) first.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="llm-provider">Language model provider</Label>
            <select
              id="llm-provider"
              value={agent.llm_provider}
              onChange={(e) =>
                setAgent((a) => ({ ...a, llm_provider: e.target.value, llm_model: "" }))
              }
              className="flex h-9 w-[180px] rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
            >
              <option value="">Select provider</option>
              {LLM_PROVIDERS.map((p) => (
                <option key={p} value={p}>
                  {p.charAt(0).toUpperCase() + p.slice(1)}
                </option>
              ))}
            </select>
          </div>
          <div className="space-y-2">
            <Label htmlFor="llm-model">Language model</Label>
            {llmModels.length > 0 ? (
              <select
                id="llm-model"
                value={agent.llm_model}
                onChange={(e) => setAgent((a) => ({ ...a, llm_model: e.target.value }))}
                className="flex h-9 w-full max-w-md rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
              >
                <option value="">Select model</option>
                {llmModels.map((m) => (
                  <option key={m.value} value={m.value}>
                    {m.label || m.value}
                    {m.default ? " (default)" : ""}
                  </option>
                ))}
              </select>
            ) : (
              <Input
                id="llm-model"
                className="max-w-md"
                value={agent.llm_model}
                onChange={(e) => setAgent((a) => ({ ...a, llm_model: e.target.value }))}
                placeholder={getLlmModelPlaceholder()}
                disabled={!agent.llm_provider}
              />
            )}
          </div>
          <div className="space-y-2">
            <Label htmlFor="system-prompt">Agent instructions</Label>
            <Textarea
              id="system-prompt"
              value={agent.system_prompt}
              onChange={(e) => setAgent((a) => ({ ...a, system_prompt: e.target.value }))}
              placeholder="Enter your agent instructions here..."
              rows={4}
              className="resize-none"
            />
            <span className="text-xs text-muted-foreground">
              {agent.system_prompt.length}/{MAX_SYSTEM_PROMPT_CHARS} characters
            </span>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-lg">Knowledge ingest</CardTitle>
          <CardDescription>
            Configure how files are ingested and stored. Set provider in OpenRAG Settings first.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="emb-provider">Embedding provider</Label>
            <select
              id="emb-provider"
              value={knowledge.embedding_provider}
              onChange={(e) =>
                setKnowledge((k) => ({
                  ...k,
                  embedding_provider: e.target.value,
                  embedding_model: "",
                }))
              }
              className="flex h-9 w-[180px] rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
            >
              <option value="">Select provider</option>
              {EMBEDDING_PROVIDERS.map((p) => (
                <option key={p} value={p}>
                  {p.charAt(0).toUpperCase() + p.slice(1)}
                </option>
              ))}
            </select>
          </div>
          <div className="space-y-2">
            <Label htmlFor="emb-model">Embedding model</Label>
            {embeddingModels.length > 0 ? (
              <select
                id="emb-model"
                value={knowledge.embedding_model}
                onChange={(e) =>
                  setKnowledge((k) => ({ ...k, embedding_model: e.target.value }))
                }
                className="flex h-9 w-full max-w-md rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
              >
                <option value="">Select model</option>
                {embeddingModels.map((m) => (
                  <option key={m.value} value={m.value}>
                    {m.label || m.value}
                    {m.default ? " (default)" : ""}
                  </option>
                ))}
              </select>
            ) : (
              <Input
                id="emb-model"
                className="max-w-md"
                value={knowledge.embedding_model}
                onChange={(e) =>
                  setKnowledge((k) => ({ ...k, embedding_model: e.target.value }))
                }
                placeholder={getEmbeddingModelPlaceholder()}
                disabled={!knowledge.embedding_provider}
              />
            )}
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label>Chunk size</Label>
              <Input
                type="number"
                min={1}
                value={knowledge.chunk_size}
                onChange={(e) =>
                  setKnowledge((k) => ({ ...k, chunk_size: Number(e.target.value) || 0 }))
                }
              />
            </div>
            <div className="space-y-2">
              <Label>Chunk overlap</Label>
              <Input
                type="number"
                min={0}
                value={knowledge.chunk_overlap}
                onChange={(e) =>
                  setKnowledge((k) => ({
                    ...k,
                    chunk_overlap: Number(e.target.value) || 0,
                  }))
                }
              />
            </div>
          </div>
          <div className="space-y-0">
            <div className="flex items-center justify-between py-3 border-b border-border">
              <div className="flex-1">
                <Label className="text-base font-medium">Table structure</Label>
                <p className="text-sm text-muted-foreground">
                  Capture table structure during ingest.
                </p>
              </div>
              <Switch
                checked={knowledge.table_structure}
                onCheckedChange={(v) =>
                  setKnowledge((k) => ({ ...k, table_structure: v }))
                }
              />
            </div>
            <div className="flex items-center justify-between py-3 border-b border-border">
              <div className="flex-1">
                <Label className="text-base font-medium">OCR</Label>
                <p className="text-sm text-muted-foreground">
                  Extracts text from images/PDFs. Ingest is slower when enabled.
                </p>
              </div>
              <Switch
                checked={knowledge.ocr}
                onCheckedChange={(v) => setKnowledge((k) => ({ ...k, ocr: v }))}
              />
            </div>
            <div className="flex items-center justify-between py-3">
              <div className="flex-1">
                <Label className="text-base font-medium">Picture descriptions</Label>
                <p className="text-sm text-muted-foreground">
                  Adds captions for images. Ingest is slower when enabled.
                </p>
              </div>
              <Switch
                checked={knowledge.picture_descriptions}
                onCheckedChange={(v) =>
                  setKnowledge((k) => ({ ...k, picture_descriptions: v }))
                }
              />
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <SettingsApp />
  </StrictMode>
);
