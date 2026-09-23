const demoProxyPath = (process.env.FEDERATION_DEMO_PROXY_PATH ?? "").replace(
  /\/+$/,
  "",
);
const federationCustomServer =
  process.env.FEDERATION_DEMO_CUSTOM_SERVER === "1";

/** @type {import('next').NextConfig} */
const nextConfig = {
  ...(federationCustomServer ? {} : { output: "standalone" }),
  assetPrefix: demoProxyPath,
  async rewrites() {
    // code-server strips its proxy prefix. Also serve these assets for direct local access.
    return demoProxyPath
      ? [
          {
            source: `${demoProxyPath}/_next/:path*`,
            destination: "/_next/:path*",
          },
        ]
      : [];
  },
  // Ray Dashboard resolves assets and API calls relative to its trailing-slash URL.
  skipTrailingSlashRedirect: true,
};

export default nextConfig;
