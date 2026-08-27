export type SettingsOwner = "coordinator" | "this-node";

export function settingsOwnerName(owner: SettingsOwner): string {
  return owner === "coordinator" ? "the fleet coordinator" : "this Forge node";
}
