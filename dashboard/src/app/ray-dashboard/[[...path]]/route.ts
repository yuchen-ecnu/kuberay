import { DemoError, getFederationService } from "@/federation/server";
import {
  DASHBOARD_PATH,
  dashboardTunnel,
  proxyDashboard,
} from "@/federation/ray-dashboard";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

async function proxy(
  request: Request,
  { params }: { params: Promise<{ path?: string[] }> },
) {
  try {
    const url = new URL(request.url);
    if (url.pathname === DASHBOARD_PATH) {
      return new Response(null, {
        status: 307,
        headers: {
          Location: `.${DASHBOARD_PATH}/${url.search}`,
          "Cache-Control": "no-store",
        },
      });
    }
    const target = await (await getFederationService()).dashboardTarget();
    const base = await dashboardTunnel.connect(target);
    return await proxyDashboard(request, base, (await params).path ?? []);
  } catch (error) {
    const status = error instanceof DemoError ? error.status : 502;
    return Response.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Dashboard proxy request failed.",
      },
      { status, headers: { "Cache-Control": "no-store" } },
    );
  }
}

export {
  proxy as GET,
  proxy as HEAD,
  proxy as POST,
  proxy as PUT,
  proxy as PATCH,
  proxy as DELETE,
};
