import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { Effect, Layer } from "effect";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import type { FleetSettings as FleetSettingsType } from "../../api/types.js";
import * as flash from "../../stores/flash.svelte.js";

const { mockUpdateFleetSettings } = vi.hoisted(() => ({
  mockUpdateFleetSettings: vi.fn(),
}));

vi.mock("../../stores/settings-workflow.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../stores/settings-workflow.js")>();
  return {
    ...actual,
    SettingsWorkflowLive: Layer.mock(actual.SettingsWorkflow)({
      updateFleet: (request) => mockUpdateFleetSettings(request),
    }),
  };
});

vi.mock("../../stores/embed-config.svelte.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../stores/embed-config.svelte.js")>()),
  isEmbedded: () => false,
}));

import FleetSettings from "./FleetSettings.svelte";
import SettingsRuntimeHarness from "./SettingsRuntimeHarness.svelte";

afterEach(() => {
  for (const item of flash.getFlashes()) flash.dismissFlash(item.id);
});

function fleetSettings(overrides: Partial<FleetSettingsType> = {}): FleetSettingsType {
  return {
    enabled: false,
    role: "coordinator",
    members: [],
    enrollments: [],
    peer_timeout: "2s",
    sessions: { include_unmanaged_details: false },
    restart_required: false,
    ...overrides,
  };
}

function renderFleetSettings(fleet: FleetSettingsType, onUpdate = vi.fn()): void {
  render(SettingsRuntimeHarness, {
    props: { component: FleetSettings, componentProps: { fleet, onUpdate } },
  });
}

describe("FleetSettings", () => {
  afterEach(() => {
    cleanup();
    mockUpdateFleetSettings.mockReset();
  });

  it("shows coordinator membership and enrollment state", () => {
    renderFleetSettings(
      fleetSettings({
        enabled: true,
        members: [
          {
            node_id: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            name: "Build node",
            base_url: "https://build.example",
            state: "active",
          },
        ],
        enrollments: [
          {
            id: "11111111111111111111111111111111",
            coordinator_base_url: "https://coordinator.example",
            coordinator_node_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            created_at: "2026-08-22T10:00:00Z",
            expires_at: "2026-08-22T11:00:00Z",
            node_base_url: "https://new-node.example",
            node_id: "cccccccccccccccccccccccccccccccc",
            node_name: "New node",
            node_platform: "linux",
            protocol_version: 3,
            state: "pending",
            updated_at: "2026-08-22T10:00:00Z",
          },
        ],
      }),
    );

    expect(screen.getByText("Federation coordinator")).toBeTruthy();
    expect(screen.getByRole("table", { name: "Federation member status" })).toBeTruthy();
    expect(screen.getByText("Build node")).toBeTruthy();
    expect(screen.getByRole("link", { name: "https://build.example" }).getAttribute("href")).toBe(
      "https://build.example",
    );
    expect(screen.getByRole("region", { name: "Enrollment activity" })).toBeTruthy();
    expect(screen.getByText("New node")).toBeTruthy();
  });

  it("shows a node's coordinator binding", () => {
    renderFleetSettings(
      fleetSettings({
        enabled: true,
        role: "node",
        coordinator: {
          node_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          name: "Studio",
          base_url: "https://studio.example",
        },
      }),
    );

    expect(screen.getByText("Execution node")).toBeTruthy();
    expect(screen.getByRole("region", { name: "Coordinator binding" })).toBeTruthy();
    expect(screen.getByText("Studio")).toBeTruthy();
    expect(screen.getByRole("link", { name: "https://studio.example" }).getAttribute("href")).toBe(
      "https://studio.example",
    );
    expect(screen.queryByRole("table", { name: "Federation member status" })).toBeNull();
  });

  it("saves runtime settings without rewriting enrollment-owned membership", async () => {
    const onUpdate = vi.fn();
    const initial = fleetSettings({
      members: [
        {
          node_id: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
          name: "Build node",
          base_url: "https://build.example",
          state: "active",
        },
      ],
    });
    const saved = fleetSettings({
      ...initial,
      enabled: true,
      peer_timeout: "4s",
      restart_required: true,
    });
    mockUpdateFleetSettings.mockReturnValue(Effect.succeed(saved));
    renderFleetSettings(initial, onUpdate);

    await fireEvent.click(screen.getByRole("checkbox", { name: "Enable fleet federation" }));
    await fireEvent.input(screen.getByLabelText("Member request timeout"), {
      target: { value: "4s" },
    });
    await fireEvent.click(screen.getByRole("button", { name: "Save fleet federation" }));

    await waitFor(() => {
      expect(mockUpdateFleetSettings).toHaveBeenCalledWith({
        enabled: true,
        peer_timeout: "4s",
        sessions: { include_unmanaged_details: false },
      });
    });
    expect(onUpdate).toHaveBeenCalledWith(saved);
    expect(screen.getByText("Restart required")).toBeTruthy();
  });

  it("surfaces save errors without discarding the draft", async () => {
    mockUpdateFleetSettings.mockReturnValue(
      Effect.fail({
        _tag: "ApiProblemError",
        operation: "save fleet settings",
        problem: {
          type: "about:blank",
          detail: "fleet.peer_timeout must be a positive duration",
        },
      }),
    );
    renderFleetSettings(fleetSettings());

    await fireEvent.input(screen.getByLabelText("Member request timeout"), {
      target: { value: "invalid" },
    });
    await fireEvent.click(screen.getByRole("button", { name: "Save fleet federation" }));

    await waitFor(() =>
      expect(flash.getFlash()).toMatchObject({
        message: "fleet.peer_timeout must be a positive duration",
        tone: "danger",
      }),
    );
    expect((screen.getByLabelText("Member request timeout") as HTMLInputElement).value).toBe("invalid");
  });
});
