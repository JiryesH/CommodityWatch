import { z, type ZodSchema } from "zod";

import { apiErrorSchema } from "@/lib/api/schemas";

const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/$/, "") ?? "/api";

export class FrontendApiError extends Error {
  status: number;
  detail?: unknown;

  constructor(message: string, status: number, detail?: unknown) {
    super(message);
    this.name = "FrontendApiError";
    this.status = status;
    this.detail = detail;
  }
}

function buildUrl(path: string) {
  if (/^https?:\/\//.test(path)) {
    return path;
  }

  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  if (apiBaseUrl.startsWith("http")) {
    return `${apiBaseUrl}${normalizedPath.replace(/^\/api/, "")}`;
  }

  if (normalizedPath.startsWith("/api")) {
    return `${apiBaseUrl === "/api" ? "" : apiBaseUrl}${normalizedPath}`;
  }

  return `${apiBaseUrl}${normalizedPath}`;
}

async function parseError(response: Response) {
  try {
    const payload = await response.json();
    const parsed = apiErrorSchema.safeParse(payload);
    if (parsed.success) {
      const message =
        parsed.data.message ??
        parsed.data.error ??
        (typeof parsed.data.detail === "string" ? parsed.data.detail : "Request failed.");
      return new FrontendApiError(message, response.status, parsed.data.detail);
    }
  } catch {
    // Ignore JSON parse failures and fall through to generic error.
  }

  return new FrontendApiError(`Request failed with status ${response.status}.`, response.status);
}

export async function getJson<T>(path: string, schema: ZodSchema<T>, init?: RequestInit): Promise<T> {
  const response = await fetch(buildUrl(path), {
    ...init,
    credentials: "include",
    headers: {
      Accept: "application/json",
      ...(init?.headers ?? {}),
    },
  });

  if (!response.ok) {
    throw await parseError(response);
  }

  const json = await response.json();
  return schema.parse(json);
}

export async function getOptionalJson<T>(path: string, schema: ZodSchema<T>, init?: RequestInit): Promise<T | null> {
  try {
    return await getJson(path, schema, init);
  } catch (error) {
    if (error instanceof FrontendApiError && error.status === 404) {
      return null;
    }

    if (error instanceof z.ZodError) {
      throw new FrontendApiError("Response validation failed.", 500, error.flatten());
    }

    throw error;
  }
}
