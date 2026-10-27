/**
 * (T211, option A) THE SALMON CARD — "Salmon · DFO", framed apart from the provincial rules, with its own as-of line
 * (owner 2026-10-27). Every species closed: one "Salmon — closed" row; otherwise the species that answer alike share a
 * row ("Sockeye · chum"). DFO places on this water not drawn yet follow, tagged "Location not yet mapped". No logic and no
 * words of its own: everything is core `salmonCard`'s.
 */
import { Text, View } from "react-native";
import type { SalmonBlockVM, SalmonCardVM, SalmonRowVM } from "@app/core";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";
import { WaterInset } from "../WaterFrame";

const chipColour = (p: Palette, s: SalmonRowVM["status"]): string =>
  s === "open" ? p.open : s === "release" ? p.restricted : s === "closed" ? p.closed : p.sub;

function Chip({ status, label, palette }: { status: SalmonRowVM["status"]; label: string; palette: Palette }) {
  return (
    <View style={{ backgroundColor: chipColour(palette, status), paddingHorizontal: 6, paddingVertical: 2,
                   borderRadius: palette.r.chip }}>
      <Text style={{ ...TYPE.pill, fontSize: 11, color: palette.onAccent }}>{label}</Text>
    </View>
  );
}

function Block({ block, palette, testID }: { block: SalmonBlockVM; palette: Palette; testID: string }) {
  return (
    <View testID={testID}>
      {block.rows.map((r, i) => (
        <View key={r.key} testID={`${testID}-row`}
              style={{ flexDirection: "row", justifyContent: "space-between", gap: 10, paddingVertical: 9,
                       borderTopWidth: i === 0 ? 0 : 1, borderColor: palette.line }}>
          <View style={{ flex: 1 }}>
            <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>
              {block.allClosed ? `${r.label} — ${r.statusLabel.toLowerCase()}` : r.label}
            </Text>
            {r.notes.map((n) => <Text key={n} style={{ ...TYPE.small, color: palette.sub }}>{n}</Text>)}
          </View>
          <Chip status={r.status} label={r.statusLabel} palette={palette} />
        </View>
      ))}
      {block.gear.map((g) => (
        <Text key={g} testID={`${testID}-gear`} style={{ ...TYPE.small, color: palette.sub, paddingTop: 2 }}>{g}</Text>
      ))}
    </View>
  );
}

export function SalmonCard({ card, palette }: { card: SalmonCardVM; palette: Palette }) {
  return (
    <WaterInset style={{ paddingTop: SPACE.s4 }}>
      <View testID="salmon-card" accessibilityLabel={`${card.title}, ${card.badge}: ${card.headline}`}
            style={{ borderWidth: 1.5, borderColor: palette.rule, borderRadius: palette.r.box, overflow: "hidden" }}>
        <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 8,
                       paddingHorizontal: 12, paddingVertical: 8, backgroundColor: palette.tint,
                       borderBottomWidth: 1, borderColor: palette.rule }}>
          <View style={{ flexDirection: "row", alignItems: "center", gap: 6 }}>
            <Text style={{ ...TYPE.name, fontSize: 17, lineHeight: 20, color: palette.ink }}>{card.title}</Text>
            <Text style={{ ...TYPE.pill, fontSize: 10, color: palette.rule, borderWidth: 1, borderColor: palette.rule,
                           paddingHorizontal: 4 }}>{card.badge}</Text>
          </View>
          <Chip status={card.headlineStatus} label={card.headline} palette={palette} />
        </View>
        <View style={{ paddingHorizontal: 12, paddingTop: 4, paddingBottom: 10 }}>
          <Block block={card} palette={palette} testID="salmon-card-block" />
          {card.notListed && (
            <Text testID="salmon-not-listed" style={{ ...TYPE.small, color: palette.ink, paddingTop: 4 }}>
              {card.notListed}
            </Text>
          )}
          {card.unmapped.map((u, i) => (
            <View key={`${u.label}#${i}`} testID="salmon-unmapped"
                  style={{ borderTopWidth: 1, borderColor: palette.line, paddingTop: 8, marginTop: 4 }}>
              <Text style={{ ...TYPE.small, color: palette.ink }}>{u.label}</Text>
              <Text testID="salmon-unmapped-tag" style={{ ...TYPE.micro, color: palette.sub }}>{u.tag}</Text>
              <Block block={u.block} palette={palette} testID="salmon-unmapped-block" />
            </View>
          ))}
          <Text testID="salmon-source" style={{ ...TYPE.metaBold, color: palette.accent, paddingTop: 6 }}>
            {card.source}
          </Text>
          <Text testID="salmon-asof" style={{ ...TYPE.small, color: palette.sub }}>{card.asOf}</Text>
          <Text testID="salmon-credit" style={{ ...TYPE.small, color: palette.sub }}>{card.credit}</Text>
        </View>
      </View>
      <Text style={{ ...TYPE.small, color: palette.sub, paddingTop: SPACE.s2 }}>{card.note}</Text>
    </WaterInset>
  );
}
