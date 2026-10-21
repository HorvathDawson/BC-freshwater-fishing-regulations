/**
 * T37 — a sheet's body: one component per `SheetBlock` kind (core T36 builds the blocks; this draws them and never
 * computes). A press inside opens the next sheet IN THIS ONE'S PLACE (`onOpen`: the prototype replaces, there is no back
 * stack); a Rule book page opens the synopsis (D6 a); "Steelhead stamp ›" goes to the Licence tab (`onTab`); a parts
 * row reads that part (`onPart`). Folds start closed.
 *
 * Ports (regs-v11.html @ 7fb6809f): the sheet CSS (`.sbody`, `.glsays`, `.glex`, `.bookw`, `.exact`, `.lbl`, `.prule`,
 * `.appl`, `.srow`, `.lvh`, `.cred`, `.tips`, `.wkstat`, `.ladder2`) as tokens.md §5 `Sheet · title|says|example|fold`;
 * the global click handler's `data-open` / `data-rule` / `data-gl` / `data-fish` / `data-howtell` (here each a run or a
 * block carrying its `SheetKey`).
 */
import { Linking, Text, View, type ImageSourcePropType } from "react-native";
import { bookUrl, type SheetBlock, type SheetKey, type SheetVM, type Tab } from "@app/core";
import { TYPE } from "../type";
import { GUTTER, type Palette } from "../theme";
import { GroupCard, Pictures } from "./GroupCard";
import { LineText, type KeepActions } from "./LineText";
import type { FishImages } from "./types";
import { Fold } from "./sheet/Fold";
import { Example, Head, List, Para, Quote } from "./sheet/Para";
import { RuleRow } from "./sheet/RuleRow";
import { LadderView } from "./sheet/LadderView";
import { SourcesView } from "./sheet/SourcesView";
import { WeekBox } from "./sheet/WeekBox";
import { Adipose, FishImage } from "./sheet/FishImage";
import { PartsChoice } from "./sheet/PartsChoice";
import { TermSection } from "./sheet/TermSection";

export interface SheetBodyProps {
  palette: Palette; fish?: FishImages;
  adipose?: { wild: ImageSourcePropType; clipped: ImageSourcePropType };
  onOpen: (k: SheetKey) => void; onPart?: (i: number) => void; onTab?: (t: Tab) => void;
  /** a Rule book page (default: open the synopsis at that page, D6 a) */
  onBook?: (pdf: number) => void;
}

const openBook = (pdf: number) => { void Linking.openURL(bookUrl(pdf)); };

/** The actions a Line's runs take inside a sheet. */
const actionsOf = (p: SheetBodyProps): KeepActions =>
  ({ onSheet: p.onOpen, onDay() {}, onTab: (t) => p.onTab?.(t), onBook: p.onBook ?? openBook });

export function SheetBlockView(p: { b: SheetBlock } & SheetBodyProps) {
  const { b, palette } = p, actions = actionsOf(p);
  const body = (bs: readonly SheetBlock[]) => bs.map((x, i) => <SheetBlockView key={i} {...p} b={x} />);
  switch (b.k) {
    case "para": return <Para text={b.text} tone={b.tone} palette={palette} actions={actions} />;
    case "example": return <Example text={b.text} palette={palette} actions={actions} />;
    case "head": return <Head text={b.text} palette={palette} />;
    case "quote": return <Quote text={b.text} palette={palette} />;
    case "list": return <List items={b.items} palette={palette} actions={actions} />;
    case "fold": return <Fold summary={b.summary} palette={palette}>{body(b.body)}</Fold>;
    case "term": return <TermSection title={b.title} palette={palette}>{body(b.body)}</TermSection>;
    case "rule": return <RuleRow title={b.title} level={b.level} applies={b.applies} book={b.book} open={b.open} palette={palette} actions={actions} />;
    case "value": return <View style={{ marginTop: 8 }}><LineText line={b.text} style={TYPE.regsBodyBold} color={palette.ink} palette={palette} actions={actions} /></View>;
    case "weekBox": return <WeekBox day={b.day} scope={b.scope} status={b.status} says={b.says} palette={palette} />;
    case "ladder": return <LadderView q={b.q} result={b.result} steps={b.steps} palette={palette} actions={actions} />;
    case "sources": return <SourcesView groups={b.groups} overruled={b.overruled} palette={palette} actions={actions} />;
    case "pictures": return <Pictures pics={b.pics} palette={palette} actions={actions} images={p.fish} />;
    case "image": return <FishImage code={b.code} file={b.file} alt={b.alt} credit={b.credit} width={b.width} height={b.height} fish={p.fish} palette={palette} />;
    case "adipose": return <Adipose figures={b.figures} images={p.adipose} palette={palette} actions={actions} />;
    case "pages": case "source": case "link":
      return <View style={{ marginTop: 8 }}><LineText line={b.line} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} linkStyle={TYPE.metaBold} /></View>;
    case "choice": return <PartsChoice items={b.items} palette={palette} onPart={p.onPart} />;
    case "group": return <View style={{ marginHorizontal: -GUTTER }}><GroupCard g={b.group} titled={b.titled} palette={palette} actions={actions} images={p.fish} /></View>;
    case "rowTitle":
      return (
        <Text style={[TYPE.lead, { color: palette.ink, marginTop: 8 }]}>
          {b.title}{b.scope ? <Text style={[TYPE.kicker, { color: palette.sub }]}>{`  ${b.scope}`}</Text> : null}
        </Text>);
  }
}

export function SheetBody(p: { vm: SheetVM } & SheetBodyProps) {
  const { vm, ...rest } = p;
  return <View testID="sheet-body">{vm.blocks.map((b, i) => <SheetBlockView key={i} {...rest} b={b} />)}</View>;
}
