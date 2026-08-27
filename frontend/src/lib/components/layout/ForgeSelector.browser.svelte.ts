import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import { render } from "vitest-browser-svelte";

import "../../../app.css";
import type { HostSummary } from "../../api/fleet-snapshot.js";
import ForgeSelectorRuntimeHarness from "./ForgeSelectorRuntimeHarness.svelte";

let snapshotHosts: HostSummary[] = [];
let originalFetch: typeof globalThis.fetch;
let unmount: (() => void) | undefined;

function host(nodeID: string, name: string, options: Partial<HostSummary> = {}): HostSummary {
  return {
    id: `host-${nodeID}`,
    configKey: nodeID,
    nodeID,
    name,
    kind: "remote",
    federationRole: "node",
    baseURL: `https://${name.toLowerCase()}.example`,
    platform: "linux",
    preferredTransport: "http",
    reachable: true,
    diagnostics: [],
    operationAvailability: {},
    tmuxSessions: [],
    ...options,
  };
}

function renderSelector(props: { compact?: boolean; fallbackLabel?: string } = {}): void {
  const view = render(ForgeSelectorRuntimeHarness, { props });
  unmount = view.unmount;
}

async function waitForDirectory(): Promise<void> {
  await vi.waitFor(() => {
    expect(document.querySelector(".forge-selector")).not.toBeNull();
  });
}

describe("ForgeSelector (browser)", () => {
  beforeEach(async () => {
    originalFetch = globalThis.fetch;
    snapshotHosts = [];
    globalThis.fetch = vi.fn(async () =>
      Response.json({
        protocolVersion: 3,
        generation: 1,
        hosts: snapshotHosts,
        projects: [],
        worktrees: [],
        sessions: [],
        workspaces: [],
      }),
    );
    await page.viewport(1280, 900);
  });

  afterEach(() => {
    unmount?.();
    unmount = undefined;
    globalThis.fetch = originalFetch;
  });

  it("stays hidden for a one-host snapshot", async () => {
    snapshotHosts = [host("self", "Local", { kind: "self", federationRole: "coordinator" })];
    renderSelector();

    await vi.waitFor(() => {
      expect(globalThis.fetch).toHaveBeenCalled();
    });
    expect(document.querySelector(".forge-selector")).toBeNull();
  });

  it("orders the coordinator first and preserves ordinary target links", async () => {
    snapshotHosts = [
      host("node-a", "Current node", { kind: "self" }),
      host("node-b", "Offline node", {
        reachable: false,
        connectionState: "offline",
      }),
      host("coordinator", "Coordinator", {
        federationRole: "coordinator",
        baseURL: "https://coordinator.example:8443",
        connectionState: "degraded",
        error: "Member health check timed out",
      }),
    ];
    renderSelector();
    await waitForDirectory();

    const trigger = page.getByLabelText("Current Forge: Current node");
    await trigger.click();
    const links = Array.from(document.querySelectorAll<HTMLAnchorElement>(".forge-selector li a"));
    expect(links.map((link) => link.querySelector("strong")?.textContent)).toEqual([
      "Coordinator",
      "Current node",
      "Offline node",
    ]);
    expect(links[0]?.getAttribute("href")).toBe("https://coordinator.example:8443");
    expect(links[0]?.getAttribute("target")).toBeNull();
    expect(links[0]?.getAttribute("onclick")).toBeNull();
    expect(links[0]?.textContent).toContain("Coordinator");
    expect(links[0]?.textContent).toContain("degraded");
    expect(links[0]?.textContent).toContain("Member health check timed out");
    expect(links[1]?.textContent).toContain("Current");
    expect(links[1]?.textContent).toContain("online");
    expect(links[2]?.textContent).toContain("offline");
  });

  it("closes before another header control opens", async () => {
    snapshotHosts = [
      host("node-a", "Current node", { kind: "self" }),
      host("coordinator", "Coordinator", { federationRole: "coordinator" }),
    ];
    const outside = document.createElement("button");
    outside.textContent = "Other header control";
    document.body.append(outside);

    try {
      renderSelector();
      await waitForDirectory();

      const trigger = page.getByLabelText("Current Forge: Current node");
      await trigger.click();
      await expect.element(page.getByRole("list", { name: "Forge fleet" })).toBeVisible();

      await page.getByRole("button", { name: "Other header control" }).click();
      await vi.waitFor(() => {
        expect(document.querySelector<HTMLDetailsElement>(".forge-selector")?.open).toBe(false);
      });
    } finally {
      outside.remove();
    }
  });

  it("removes hosts omitted by a later authoritative snapshot", async () => {
    const coordinator = host("coordinator", "Coordinator", {
      federationRole: "coordinator",
    });
    const current = host("node-a", "Current node", { kind: "self" });
    const removed = host("node-b", "Removed node");
    snapshotHosts = [current, coordinator, removed];
    renderSelector();
    await waitForDirectory();

    const trigger = page.getByLabelText("Current Forge: Current node");
    await trigger.click();
    await expect.element(page.getByText("Removed node", { exact: true })).toBeVisible();
    await trigger.click();

    snapshotHosts = [current, coordinator];
    await trigger.click();
    await vi.waitFor(() => {
      expect(document.body.textContent).not.toContain("Removed node");
    });
  });

  it("fits the compact selector within a phone viewport", async () => {
    await page.viewport(375, 700);
    snapshotHosts = [
      host("node-a", "Current node with a long name", { kind: "self" }),
      host("coordinator", "Coordinator with a long name", {
        federationRole: "coordinator",
      }),
    ];
    renderSelector({ compact: true, fallbackLabel: "kenn-forge" });
    await waitForDirectory();

    const trigger = page.getByLabelText("Current Forge: Current node with a long name");
    await trigger.click();
    const triggerBox = trigger.element().getBoundingClientRect();
    const menuBox = document.querySelector(".forge-selector ul")?.getBoundingClientRect();
    expect(triggerBox.left).toBeGreaterThanOrEqual(0);
    expect(triggerBox.right).toBeLessThanOrEqual(375);
    expect(menuBox).toBeDefined();
    expect(menuBox?.left).toBeGreaterThanOrEqual(0);
    expect(menuBox?.right).toBeLessThanOrEqual(375);
  });
});
