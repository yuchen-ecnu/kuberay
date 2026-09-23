import { NextResponse } from "next/server";
import { DemoError, getFederationService } from "@/federation/server";
import { MAX_YAML_BYTES } from "@/federation/model";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function errorResponse(error: unknown) {
  return NextResponse.json(
    {
      error:
        error instanceof Error ? error.message : "The demo request failed.",
    },
    {
      status: error instanceof DemoError ? error.status : 400,
      headers: { "Cache-Control": "no-store" },
    },
  );
}

export async function GET() {
  try {
    return NextResponse.json(await (await getFederationService()).snapshot(), {
      headers: { "Cache-Control": "no-store" },
    });
  } catch (error) {
    return errorResponse(error);
  }
}

export async function POST(request: Request) {
  try {
    // Stream with a bound; Content-Length is not trusted.
    const reader = request.body?.getReader();
    if (!reader) throw new DemoError("The request body is empty.");
    const chunks: Uint8Array[] = [];
    let length = 0;
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      length += value.length;
      if (length > MAX_YAML_BYTES * 2) {
        await reader.cancel();
        throw new DemoError("The request is too large.", 413);
      }
      chunks.push(value);
    }
    const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    if (
      typeof body.yaml !== "string" ||
      typeof body.revision !== "string" ||
      (body.target !== undefined &&
        !["frc", "primary"].includes(body.target)) ||
      !["validate", "apply"].includes(body.action)
    ) {
      throw new DemoError(
        "Provide yaml, revision and a validate or apply action.",
      );
    }
    return NextResponse.json(
      await (
        await getFederationService()
      ).update(
        body.yaml,
        body.revision,
        body.action === "validate",
        body.target ?? "frc",
      ),
      { headers: { "Cache-Control": "no-store" } },
    );
  } catch (error) {
    return errorResponse(error);
  }
}
