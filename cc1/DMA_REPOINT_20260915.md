# DMA-repoint 15-09-2026 — meetnotitie

Op **15 september 2026** is in de DMA-accounts van 316 campagnes het targeting-id (custom label 0 /
`product_custom_attribute` INDEX0) aangepast of de campagne gepauzeerd. Deze notitie is bedoeld om
later te kunnen meten wat die ingreep heeft opgeleverd. Achtergrond en de API-valkuilen staan in
`LEARNINGS.md` (2026-09-15), het verloop in `TASKS.md` (2026-09-15 (4)).

De campagnelijst met baseline per campagne staat in **`DMA_REPOINT_20260915_campagnes.csv`**
(316 regels). Snapshots van alle originele productgroepbomen + `repair.py` voor terugdraaien staan
buiten de repo in `Downloads\claude\dma_repoint_20260915\`.

## Wat er precies gewijzigd is

| Groep | Campagnes | Wat | Status na 15-09 |
|---|---|---|---|
| `omgezet_actief` | 213 | Targeting-id naar een geldige biedcategorie, boom verder identiek | ENABLED |
| `omgezet_daarna_gepauzeerd` | 13 | Omgezet, daarna alsnog gepauzeerd (doel-id leeg of dubbel) | PAUSED |
| `gepauzeerd` | 90 | Niet omgezet (categorie bestaat niet meer), alleen gepauzeerd | PAUSED |

De 213 uit `omgezet_actief` zijn de enige die iets kunnen gaan doen. Verdeling: **186 DMA NL**
(`3800751597`) en **27 DMA BE** (`9920951707`), in drie rondes — `NL1 taxv2-naammatch` (120),
`NL2 zustercampagne` (66) en `BE` (27). De kolom `ronde` in de CSV houdt dat uit elkaar; de
NL2-groep heeft het hardste doel-id (overgenomen van een levende zustercampagne) en is dus de
schoonste subgroep om mee te beginnen.

## De baseline om tegen af te zetten

Laatste normale jaar = **dec 2024 t/m nov 2025**, want in november 2025 vielen ze stil.

| Groep | Kosten | Omzet (`all_conversions_value`) | Kliks |
|---|---|---|---|
| `omgezet_actief` (213) | € 69.683 | € 110.960 | 674.440 |
| `omgezet_daarna_gepauzeerd` (13) | € 105 | € 146 | 1.698 |
| `gepauzeerd` (90) | € 73 | € 114 | 1.208 |

Dat is **€ 5.807 kosten en € 9.247 omzet per maand** voor de 213 — ROAS ≈ 159%. Terugkomen op dat
niveau is het maximum dat deze ingreep kan opleveren; minder is normaal, want de categorieën zijn
tien maanden niet gezien en het assortiment is gewijzigd.

Eerste signaal op de dag zelf: **48 NL-campagnes gaven samen € 21,93 uit**, BE nog niets. In BE zijn
wel ruim 5.400 producten van `not_eligible_in_any_campaign` naar eligible gegaan (Pyjama's 2.013,
Geursets +697, Badjassen 952, Nachthemden 447, Kraamcadeaus 325).

## Hoe je het meet

```sql
-- per campagne, vergelijk een periode na 15-09 met dezelfde lengte uit de baseline
SELECT campaign.id, campaign.name, segments.date,
       metrics.cost_micros, metrics.clicks, metrics.all_conversions_value
FROM campaign
WHERE campaign.id IN (<campaign_id's uit de CSV, groep = omgezet_actief>)
  AND segments.date BETWEEN '2026-09-16' AND '<einddatum>'
```

Let bij de uitleg op vier dingen:

1. **Niet eerder dan 72 uur na de start beoordelen** — de CPR-conversies komen met ~2 dagen
   vertraging binnen, dus verse dagen zien er structureel te slecht uit.
2. **Leerfase.** Alle 213 draaien op tROAS en hebben tien maanden geen data gezien. Reken op
   onrustige biedingen in de eerste weken; een uitschieter zegt dan meer over de leerfase dan over
   de categorie.
3. **Vergelijk met de juiste maanden.** Sep-nov 2025 tegen sep-nov 2026 is eerlijker dan tegen het
   jaargemiddelde, want de baseline bevat de piekmaanden okt/nov.
4. **Kannibalisatie.** Een deel van het verkeer is na nov 2025 door de shopcampagnes opgepikt.
   Stijging op deze 213 is dus niet automatisch netto nieuwe omzet — kijk ook naar het DMA-totaal
   per land, niet alleen naar deze campagnes.

## Wat er NIET mee verklaard wordt

- De 103 gepauzeerde campagnes kostten samen € 178 in het laatste normale jaar. Daar valt niets te
  meten; die zijn gepauzeerd om het account leesbaar te maken.
- Het echte omslagpunt in november 2025 was niet het dode categorie-id maar het uitsluiten van de
  "Overig"-node. Zie `LEARNINGS.md`. Als die node ooit terugkomt verandert het speelveld opnieuw.

## Open punten

- [ ] Meten vanaf ± 1 oktober 2026: kosten/omzet/ROAS van de 213 tegen de baseline hierboven.
- [ ] Vraag aan DM: wie heeft in november 2025 de Overig-node accountbreed uitgesloten?
- [ ] **5.537 ENABLED campagnes in DMA NL staan op een overzichtscategorie**
      (`isBiddingCategory = false`) — nul daarvan gaf in 2026 iets uit, tegen 87% van de 2.769 op een
      echte biedcategorie. Kost niets, maar maakt het account onleesbaar. Uitzoeken of ze ooit wél
      gedraaid hebben en of een script ze blijft aanmaken.
