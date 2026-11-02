# Alpheus — implementeringsspecifikation (Go)

Status: udkast, 2026-11-02. Trin 1–8 i afsnit 15 er bygget: API-typer og
CRD, chunks og lager, komponent-SDK'et, agenten, controlleren og
end-to-end-tests på Kubernetes (Talos i en VM) med fan-out, fan-in,
replikaer, genstart, backpressure og kaos, og fejltests (testcase09 til
testcase15: lageret går ned, controlleren dør, afviste nøgler, fuld
bucket, for lille scratch-mappe, en komponent med en fejl, set gennem UI'et
i klyngen). Fejlsøgning (GO-138 til GO-143) er bygget: årsagskoder, events,
gemte logs, genkørsel af et fejlet forsøg, `alpheus ui` på maskinen og i
klyngen, og metrics. Af trin 9 er CLI, Helm-chart,
skabeloner til komponenter i Go og Python og en guide til at skrive
komponenter, flagskibet (ETL af store filer) og udgivelsesscriptet
bygget; tilbage er infrastrukturen og første udgivelse. Åbne spørgsmål
står i afsnit 16.

Kravene GO-88 til GO-124 gør Alpheus formatuafhængigt: chunks er filer,
som Alpheus flytter uden at læse dem, og Parquet og Arrow er ude af
Alpheus.

Denne specifikation beskriver, hvordan Alpheus skrives i Go. Den bygger på
den funktionelle specifikation "Alpheus — specifikation" (StreamRun-CRD,
komponent-kontrakt, chunk-protokol, signal-drivere, integration med MCPipe,
drift og accepttest) og gentager ikke kravene derfra. Krav-ID'er herfra
starter med `GO-`.

Hvor denne specifikation ændrer den funktionelle, gælder denne, og den
funktionelle bør rettes:

| Funktionelt krav | Ændret af | Ændring |
|---|---|---|
| KK-1 | 7.1 | Komponenter ser lokale filer i deres egen scratch-mappe, aldrig S3, beskeder eller andre pods' filer |
| CH-1 | 7.3 | Manifestet er commit-punktet; chunks skrives under deres endelige navn, ikke med midlertidigt navn og omdøbning |
| Signal-drivere | 8 | Kun `builtin`; STOMP-driveren er udgået (GO-39, GO-42) |
| Dataformat | GO-101 | Alpheus læser aldrig chunks; Parquet og Arrow er ude |

Krav bruger SKAL og BØR.


## 1. Overblik

Alpheus består af fire programmer og ét bibliotek, alle i ét Go-modul:

| Del | Binær / pakke | Kører som |
|---|---|---|
| Controller | `alpheus-controller` | Deployment i `alpheus-system` |
| Agent | `alpheus-agent` | Native sidecar i hver trin-pod |
| CLI | `alpheus` | Brugerens maskine eller CI |
| Signal-hub | indbygget i controlleren | Del af `alpheus-controller` |
| Komponent-SDK | `alpheus-dev.org/alpheus/sdk` | Importeres af komponenter |


## 2. Modul, sprog og byggeregler

| ID | Niveau | Krav |
|---|---|---|
| GO-1 | SKAL | Ét Go-modul med stien `alpheus-dev.org/alpheus`. Stien serveres som vanity import fra hjemmesiden (se `SPEC.md` i hjemmesidens repo), så modulet ikke er bundet til GitHub eller Forgejo. |
| GO-2 | SKAL | Go-versionen fastlåses i `go.mod` (`go` og `toolchain`). Projektet følger den seneste stabile Go-udgivelse og understøtter den forrige. |
| GO-3 | SKAL | Alle binærer bygges med `CGO_ENABLED=0` som statiske binærer. |
| GO-4 | SKAL | Byggeriet er reproducerbart: binærer bygges med `-trimpath` og `-buildvcs=true`, og image-lag og arkiver bruger fast `SOURCE_DATE_EPOCH` (commit-tidspunktet). To byggerier af samme commit giver samme binær, samme arkiver, SBOM'er og chart og samme digests for images og chart. |
| GO-5 | SKAL | Container-images bygges på `gcr.io/distroless/static` eller `scratch`, kører som non-root (UID 65532) og har read-only rodfilsystem. |
| GO-6 | SKAL | Images bygges til `linux/amd64` og `linux/arm64`. |
| GO-7 | SKAL | Hver udgivelse har en SBOM (SPDX) og signerede images, binærer og chart. Der signeres med cosign og projektets egen nøgle, ikke keyless, så ingen signatur afhænger af Sigstores offentlige tjenester uden for EU (se 14). |
| GO-8 | SKAL | Versionering følger SemVer. CRD-versionen (`v1alpha1`, `v1beta1`, `v1`) er uafhængig af programversionen. |
| GO-9 | BØR | Ét script i repo'et bygger binærer, images, SBOM og checksums, så en udgivelse kan genskabes uden for CI. |
| GO-88 | SKAL | `go.mod` ligger i repo'ets rod, så modulstien `alpheus-dev.org/alpheus` peger på roden af repo'et. |
| GO-89 | SKAL | Udgivelser tagges `vX.Y.Z` på `main`. CI kontrollerer efter hver udgivelse, at `go list -m alpheus-dev.org/alpheus@vX.Y.Z` finder den. |


## 3. Repo-struktur

Go-modulet ligger i repo'ets rod (`go.mod` i roden), så modulstien
`alpheus-dev.org/alpheus` svarer direkte til repo'et. Testklyngen ligger i
`vagrant/` og dokumentationen i `docs/`; Go ignorerer mapper uden Go-filer.
Hjemmesiden, dens specifikation og logoet ligger i sit eget repo,
`alpheus-dev.org`, som henter `docs/` herfra ved byg (WWW-22).
Mapper markeret *(findes)* er bygget; resten er planlagt.

```
alpheus/
├── SPEC.md                denne specifikation (findes)
├── Makefile               generate, test, test-crd, test-chart, cli, e2e, bench, bench-etl, release og flere (findes)
├── buf.gen.yaml           kodegenerering fra proto/ (findes)
├── api/v1alpha1/          StreamRun-typer med kubebuilder-markører (findes)
├── cmd/
│   ├── alpheus/           CLI'ens binær (findes)
│   ├── alpheus-controller/ controllerens binær (findes)
│   └── alpheus-agent/     agentens binær (findes)
├── internal/
│   ├── cli/               CLI'ens kommandoer (findes)
│   ├── controller/        reconcile-løkke, pod-spec, status, signal-CA og hub (findes)
│   ├── agent/             socket-server, ind- og udgange, genstart (findes)
│   ├── chunk/             navngivning, manifest, ack-filer, checksum (findes)
│   ├── pb/                genereret Go-kode fra proto/ (findes)
│   ├── signal/            driver-interface (findes)
│   │   └── builtin/       gRPC-hub i controlleren og klient i agenten (findes)
│   ├── store/             S3-klient bag et lille interface (findes)
│   └── version/
├── sdk/                   offentligt komponent-SDK, lokal mode og agent-mode (findes)
├── proto/                 component.proto og signal.proto (findes)
├── config/                genererede CRD-, RBAC- og webhook-manifester (CRD og RBAC findes)
│   ├── crd/bases/         StreamRun-CRD'en (findes)
│   └── samples/           eksempler, bl.a. testcase03 (findes)
├── hack/                  boilerplate, test-crd.sh, test-chart.sh, images.sh, e2e.sh, release.sh, verify-release.sh og clireference (findes)
├── charts/alpheus/        Helm-chart: CRD, RBAC, controller og hub-Service (findes)
├── templates/             skabeloner til komponenter: component-go og component-python (findes)
├── examples/log-report/   eksemplet fra komponent-guiden, i Go og Python (findes)
├── examples/meter-etl/    flagskibet: ETL af store filer med elmåler-aflæsninger (findes)
├── test/
│   ├── components/        testkomponenter: bench (findes), filter og sum i CSV (findes)
│   ├── e2e/               testcase01 til testcase15 (findes)
│   └── internal/table/    CSV og decimaltal for testkomponenterne (findes)
├── vagrant/               testklyngen: Talos i en VM med Garage og Zot (findes)
├── docs/                  dokumentationen, som hjemmesiden henter ved byg (findes)
├── GOVERNANCE.md
├── SECURITY.md
└── LICENSE                Apache 2.0 (findes)
```

Alt under `internal/` er privat. Kun `api/` og `sdk/` er offentlig API og
følger SemVer-garantier.


## 4. Afhængigheder

Hver afhængighed skal godkendes, før den tages i brug. Kolonnen Status
siger, om den er godkendt og i brug (med versionen i `go.mod`), eller
stadig kun foreslået.

| Formål | Bibliotek | Licens | Status |
|---|---|---|---|
| Kubernetes-klient og controller | `sigs.k8s.io/controller-runtime`, `k8s.io/client-go` | Apache 2.0 | Godkendt, i brug (v0.25.2; k8s.io v0.37.0) |
| CRD-generering | `sigs.k8s.io/controller-tools` (kun build) | Apache 2.0 | Godkendt, i brug (v0.23.0, `go tool`) |
| S3 | `github.com/minio/minio-go/v7` | Apache 2.0 | Godkendt, i brug (v7.4.1) |
| gRPC | `google.golang.org/grpc`, `google.golang.org/protobuf` | Apache 2.0 / BSD | Godkendt, i brug (v1.83.2 og v1.36.12) |
| Protobuf-generering | `protoc-gen-go`, `protoc-gen-go-grpc` (kun build, `go tool`), kørt af `buf` | BSD / Apache 2.0 | Godkendt sammen med gRPC (v1.36.12 og v1.6.2) |
| CLI | `flag` fra standardbiblioteket; YAML med `sigs.k8s.io/yaml`, som Kubernetes-klienten allerede trækker med | BSD / Apache 2.0 | I brug. Cobra blev ikke godkendt |
| Metrics | `github.com/prometheus/client_golang` | Apache 2.0 | Godkendt, i brug (v1.24.0) |
| Logning | `log/slog` fra standardbiblioteket | BSD | I brug |
| Test | `sigs.k8s.io/controller-runtime/pkg/envtest`, `setup-envtest` (kun build, `go tool`) | Apache 2.0 | Godkendt, i brug (v0.25.2; Kubernetes 1.37.0) |
| Sårbarheder | `golang.org/x/vuln` (`govulncheck`, kun build, `go tool`) | BSD | Godkendt, i brug (v1.9.0). Databasen hentes fra vuln.go.dev (USA) med `make vulndb`; den er offentlige data og kan spejles i EU |

| ID | Niveau | Krav |
|---|---|---|
| GO-10 | SKAL | Kun licenser, der er forenelige med Apache 2.0. Ingen GPL, AGPL, BSL eller "community"-licenser. |
| GO-11 | SKAL | `govulncheck` blokerer en udgivelse ved kendte sårbarheder i kode, Alpheus kalder: `hack/release.sh` kører den uden netværk mod en lokal kopi af Gos sårbarhedsdatabase, højst `VULNDB_MAX_AGE` dage gammel (standard 7), og `make vuln` kører den samme kontrol. `make vulndb` henter kopien fra vuln.go.dev eller et spejl (`VULNDB_URL`). Kontrollen kører også i CI, når CI findes (GO-54). |
| GO-12 | BØR | minio-go vælges frem for AWS SDK, fordi den er mindre og testet mod S3-kompatible lagre som Garage. Begge ligger bag `internal/store`, så valget kan ændres. |

Parquet og Arrow (`github.com/apache/arrow-go`) er ikke en afhængighed
(GO-101). Parquet og DuckDB findes kun i MCPipe.


## 5. API-typer (`api/v1alpha1`)

| ID | Niveau | Krav |
|---|---|---|
| GO-13 | SKAL | API-gruppen er `alpheus-dev.org`, versionen `v1alpha1`, kind `StreamRun`, kort navn `sr`. |
| GO-14 | SKAL | Typerne har kubebuilder-markører for validering (påkrævede felter, mønstre, min/max), så CRD'en genereres med OpenAPI-skema. |
| GO-15 | SKAL | `spec` er uforanderlig efter oprettelse, bortset fra `spec.cancel`. Håndhæves med CEL-validering i CRD'en (`x-kubernetes-validations`), ikke kun i controlleren. |
| GO-16 | SKAL | Status-subresource er slået til, og kun controlleren skriver status. |
| GO-17 | SKAL | Printer-kolonner viser fase, antal trin, alder og eventuel fejl i `kubectl get sr`. |
| GO-18 | BØR | Conditions følger `metav1.Condition` (`Ready`, `Progressing`, `Degraded`). |

Felter i CRD'en fra kravene GO-78 til GO-124:

| Felt | Krav |
|---|---|
| `spec.resultsTTL` (standard 7 dage, `0` = aldrig) | GO-78 |
| `spec.steps[].failurePolicy` (`stop` eller `continue`) og fasen `PartiallySucceeded` | GO-97 |
| `spec.chunks.maxBytes` | GO-105 |
| `spec.steps[].scratch` (standard 10 GiB) | GO-106 |
| `spec.steps[].maxOpen` (standard 8) | GO-110 |
| `spec.steps[].prefetch` (standard 2) | GO-111 |
| `spec.steps[].inputOrder` (`arrival` eller `deterministic`) | GO-112 |
| `spec.edges[].port` (standard `default`) | GO-108 |
| `status.steps[].replicas[].failedChunks` | GO-97 |
| `status.steps[].counts`, `errors`, `warnings`, `errorsDropped`, `errorLog` | GO-115, GO-119, GO-120 |
| `spec.chunks.maxRows` fjernes | GO-86 |

### 5.1 Replikaer

Et trin kan køre i flere kopier, der deler input-chunks mellem sig:

```yaml
steps:
  - name: generate
    image: registry.example/generate@sha256:…
  - name: filter
    image: registry.example/filter@sha256:…
    replicas: 10
    settings: {field: country, in: [DK]}
  - name: sum
    image: registry.example/sum@sha256:…
edges:
  - {from: generate, to: filter}
  - {from: filter, to: sum}
```

Hver replika er sin egen pod med sin egen agent. Hver producent-replika
skriver sin egen strøm af chunks på en kant (7.3), så der stadig kun er
én skriver pr. manifest. Et trin uden `replicas` har én replika.

| ID | Niveau | Krav |
|---|---|---|
| GO-75 | SKAL | `spec.steps[].replicas` (standard 1, højst 64) angiver antallet af replikaer. Replikaerne nummereres 0 til `replicas`-1, og agenten får sit nummer som miljøvariabel. |
| GO-76 | SKAL | Chunk nummer n i strømmen fra producent-replika p går til forbruger-replika (n + p) mod `replicas` hos hvert forbrugende trin. Leddet p spreder arbejdet, når mange producent-replikaer hver laver få chunks; uden det ville alle strømme starte på samme forbruger-replika. Fordelingen er deterministisk, så en genstartet replika får præcis de samme chunks igen. |
| GO-77 | SKAL | `maxRestarts` og forsøgsnummeret (GO-67) gælder pr. replika. Et trin er afsluttet, når alle dets replikaer er afsluttet, og fejlet, når én replika har brugt alle sine genstarter. |
| GO-97 | SKAL | Har en replika brugt alle sine genstarter, og er `failurePolicy` `continue`, lukker controlleren replikaens udgående strømme. Først når pod'en er bekræftet væk (GO-67), skriver controlleren hvert udgående manifest med `eos: true`, uændrede chunks og et `epoch` én højere end replikaens sidste forsøg, så en agent, der mod forventning stadig kører, stoppes af fencing (GO-68). Controlleren lukker også replikaens ack-filer: de lister de chunks, replikaen committede, har `closed: true` og samme højere `epoch`. En producent venter ikke på en lukket forbruger-replika (GO-35), så runnet går ikke i stå, og en agent, der stadig kører, stoppes ved sin næste skrivning af ack-filen. Input-chunks, der var tildelt replikaen, men ikke står i dens lukkede ack-fil, tælles i `status.steps[].replicas[].failedChunks`, også dem, producenten laver, efter at replikaen er lukket. Forbrugerne når EOS, og runnet slutter som `PartiallySucceeded`. Med `failurePolicy: stop` stoppes hele runnet i stedet. |

### 5.2 Resultater

En udgang, som ingen kant går fra, er et resultat. Agenten skriver den som
en strøm med manifest, ligesom en kant, men under
`results/<run>/<uid>/<trin>/<udgang>/<replika>/` i stedet for
`runs/<uid>/`. `<uid>` er StreamRun-objektets `metadata.uid`.

| ID | Niveau | Krav |
|---|---|---|
| GO-83 | SKAL | Stier i lageret bruger runnets UID, ikke kun dets navn: `runs/<uid>/` og `results/<run>/<uid>/`. Slettes en StreamRun og oprettes igen med samme navn, får den en ny UID, og gamle og nye data blandes aldrig. Navnet står med i `results/` og i manifestet (`run`), så en person kan finde resultatet. |
| GO-78 | SKAL | Resultater slettes ikke af `ttlAfterFinished` eller finalizeren (GO-20). De slettes, når `spec.resultsTTL` er udløbet, regnet fra `finishedAt`. Standard er 7 dage. Værdien `0` betyder, at controlleren aldrig sletter dem, og skal angives eksplicit. |
| GO-98 | SKAL | Agenten skriver `results/<run>/<uid>/run.json` med runnets navn, namespace, UID, `finishedAt` og `resultsTTL`, før den første resultat-chunk skrives. Så kan resultater findes og ryddes, også efter at StreamRun-objektet er slettet. |
| GO-129 | SKAL | Resultater kan ligge i deres egen bucket: `spec.storage.resultsBucket` og `spec.storage.resultsSecretRef`. Agenterne skriver dem med `secretRef`, der skal kunne skrive begge buckets, og `status.results` peger på `resultsSecretRef`, en nøgle, der kun kan læse resultat-bucketen. Uden `resultsBucket` ligger resultater i `bucket`, og `status.results` peger på `secretRef`, der kan skrive hele bucketen; dokumentationen siger det tydeligt. Nøgler i S3-lagre som Garage gælder pr. bucket, ikke pr. prefix, så en egen bucket er den måde, læsere kan nøjes med læseadgang. Alt under `results/<run>/<uid>/` ligger i resultat-bucketen: resultaterne, `run.json`, resuméet og fejlloggen (`_errors/`); `runs/<uid>/` bliver i `bucket`. Agent og controller bruger ét lager, der sender nøgler efter prefix (`store.WithResults`), og agenten får bucketen i `ALPHEUS_S3_RESULTS_BUCKET`. `resultsSecretRef` kræver `resultsBucket` (CEL). |
| GO-99 | BØR | `alpheus results gc` finder resultater, hvis TTL er udløbet, regnet fra `finishedAt` i `run.json`, og sletter dem efter bekræftelse (eller med `--yes`). Resultater, hvis StreamRun-objektet ikke længere findes, vises, men slettes kun med `--orphans`, da resultater er beregnet til at overleve objektet (GO-78). Resultater med `resultsTTL` 0 og uden `run.json` slettes aldrig. Med `--dry-run` vises kun, hvad der ville blive slettet. |
| GO-79 | SKAL | `status.results` har én post pr. resultat med S3-endpoint, bucket, region, prefix, navnet på Secret'en med nøglerne til at læse det og antal chunks, bytes og komponentens committede tællere (10.1). Det er det, der skal til for at hente resultatet, og `alpheus get` viser det. |

Secret'en i `status.results` er den samme som agenternes, med adgang til
hele bucketen. Det strider mod princippet om, at agentens nøgle kun
giver adgang til runnets eget prefix. I testmiljøet er det acceptabelt;
en særskilt læsenøgle kun til `results/` er et åbent spørgsmål (16).


## 6. Controller (`internal/controller`)

### 6.1 Reconcile-løkken

Controlleren reconciler én StreamRun ad gangen pr. worker og er
idempotent: samme input giver samme ønskede tilstand.

1. Valider grafen: navne unikke, kanter peger på eksisterende trin,
   ingen løkker. Fejl sætter fasen `Failed` med en læsbar årsag.
2. Opret Secret-referencer, ServiceAccount og NetworkPolicy for runnet,
   hvis de mangler.
3. Opret én pod pr. replika af hvert trin, hvis den mangler (se 6.2).
4. Læs pod-status og agenternes rapporter, og opdater `status`.
5. Når alle trin er afsluttet, sæt slutfasen og `finishedAt`.
6. Når `ttlAfterFinished` er udløbet, slet pods og runnets S3-prefix
   (`runs/<uid>/`, ikke `results/`, GO-78).

| ID | Niveau | Krav |
|---|---|---|
| GO-19 | SKAL | Alle objekter, et run opretter, har `ownerReferences` til StreamRun-objektet. |
| GO-20 | SKAL | En finalizer (`alpheus-dev.org/cleanup`) sikrer, at runnets S3-prefix slettes, før objektet forsvinder. Oprydningen kræver nøglerne, og når et namespace slettes, slettes Secret'en med dem samtidig. Derfor sætter controlleren også en finalizer pr. run (`alpheus-dev.org/storage-<uid>`) på storage-Secret'en og fjerner den først, når prefixet er slettet; runs, der deler Secret'en, holder den hver især. Findes Secret'en ikke, fx fordi den aldrig fandtes, logger controlleren, at data bliver liggende, og fjerner runnets finalizer, så objektet ikke hænger. |
| GO-21 | SKAL | Controlleren bruger leader election, så to replicas kan køre samtidig. |
| GO-22 | SKAL | Statusopdateringer bruger `Patch` med optimistisk låsning og rate-begrænses (højst én pr. sekund pr. run). |
| GO-23 | BØR | En validating webhook afviser ugyldige grafer, før objektet gemmes. Uden webhook sker samme validering i reconcile-løkken. |

### 6.2 Pod-spec pr. trin

- Komponenten er den eneste almindelige container.
- Agenten kører som native sidecar: en init-container med
  `restartPolicy: Always`. Den starter før komponenten og stopper efter
  den. Det kræver Kubernetes 1.29 eller nyere (beta og slået til som
  standard fra 1.29, GA fra 1.33).
- Agent og komponent deler en `emptyDir` med Unix-socketten
  (`/run/alpheus/agent.sock`) og en lokal scratch-mappe til chunks.
- Pod'en har `restartPolicy: Never`. Genstart af et fejlet trin
  håndteres af controlleren, så antallet kan tælles og begrænses
  (`spec.steps[].maxRestarts`, standard 3).
- Kun agenten får S3- og signal-legitimation. Komponenten får kun de
  Secrets, der står i `envFrom`.
- securityContext på begge containere: `runAsNonRoot`,
  `readOnlyRootFilesystem`, `allowPrivilegeEscalation: false`, alle
  capabilities fjernet, seccomp `RuntimeDefault`.

| ID | Niveau | Krav |
|---|---|---|
| GO-24 | SKAL | Agent-imaget og dets digest er konfigureret i controlleren, ikke i StreamRun. Alle trin i et run bruger samme agent-version. |
| GO-25 | SKAL | Pod-specs opfylder Pod Security-niveauet `restricted`. |
| GO-26 | SKAL | Controlleren afviser komponent-images uden digest, medmindre installationen eksplicit tillader tags (til udvikling). |
| GO-67 | SKAL | Hvert forsøg på et trin har et forsøgsnummer (`status.steps[].attempt`), som agenten får som miljøvariabel. Controlleren starter først et nyt forsøg, når det forrige forsøgs pod er bekræftet væk: pod'en er slettet, eller alle dens containere er `Terminated`. En pod i fasen `Unknown` (fx en node uden forbindelse) blokerer et nyt forsøg, indtil pod'en er slettet. Sletning tæller kun som bekræftelse, når kubelet har fuldført den. En tvungen sletning (`--grace-period=0 --force`) fjerner objektet, mens containerne måske stadig kører; controlleren kan ikke se forskel, så det er brugerens ansvar, og fencing (GO-68) fanger en dobbelt skriver. |


## 7. Agent (`internal/agent`)

### 7.1 Ansvar

Agenten er den eneste del, der taler med S3 og signal-driveren. Den:

- tilbyder komponent-API'et på Unix-socketten (`proto/component.proto`),
- læser input-chunks fra S3 og lægger dem som filer i komponentens
  scratch-mappe,
- uploader komponentens output-filer som chunks til S3 og opdaterer
  manifestet,
- sender og modtager `ready`, `ack`, `eos` og `fail`,
- håndhæver backpressure (`maxUnacked`) og genstart fra manifestet,
- rapporterer tællere til controlleren og som Prometheus-metrics.

### 7.2 Komponent-API (`proto/component.proto`)

Alpheus flytter filer mellem containere uden at læse dem. Data går som
filer i pod'ens scratch-mappe; Unix-socketten bærer kun kontrolkald.

```proto
service Agent {
  rpc Next(NextRequest) returns (NextResponse);        // næste input-chunk som lokal fil, eller EOS
  rpc Create(CreateRequest) returns (CreateResponse);  // ny output-chunk som lokal fil på en udgang
  rpc Close(CloseRequest) returns (CloseResponse);     // output-chunken er skrevet færdig
  rpc Done(DoneRequest) returns (DoneResponse);        // input-chunken er færdigbehandlet
  rpc Observe(stream ObserveRequest) returns (ObserveResponse); // tællere, fejl, fremdrift (10.1)
  rpc Finish(FinishRequest) returns (FinishResponse);  // komponenten er færdig; alle udgange afsluttes
}
```

| ID | Niveau | Krav |
|---|---|---|
| GO-27 | — | Udgået: erstattet af GO-107. |
| GO-28 | — | Udgået: Alpheus kender ikke skemaer. Skemavalidering er komponentens eller MCPipes ansvar. |
| GO-29 | SKAL | Hvis komponenten ikke har kontaktet agenten (et kald eller `Observe`-strømmen) inden for en konfigurerbar tid efter start (standard 5 minutter), markeres trinnet som fejlet. En kilde kalder aldrig `Next`, så kontakt tæller i stedet for `Next`. |
| GO-125 | SKAL | `Finish` siger, at komponenten er færdig: den har læst al sin input, og output fra `OnEOS` er lukket. Agenten afviser `Finish`, hvis der er input-chunks, der ikke er læst eller ikke er `Done`. Derefter skriver agenten `eos` i alle udgående manifester (GO-80, GO-91). En komponent, der stopper uden `Finish`, har fejlet, og dens udgange afsluttes ikke. |
| GO-126 | SKAL | Hver besked på `Observe` har et løbenummer, og `Done`, `Close` og `Finish` angiver nummeret på den sidste besked, komponenten sendte før kaldet. Agenten venter på den besked, før den committer. gRPC garanterer ikke rækkefølgen på tværs af strømme, så uden nummeret kunne tællere fra en chunk komme med i den næste. |
| GO-127 | SKAL | `Next` angiver, om komponenten har registreret `OnEOS` (GO-95). Det må ikke ændre sig efter første `Next`; agenten afviser det med en brugsfejl. Agenten venter med at hente input til første `Next`, fordi genstart af et tilstandsfuldt trin begynder forfra. |
| GO-128 | SKAL | Agenten får sin konfiguration fra controlleren som JSON i `ALPHEUS_AGENT_CONFIG` (run, UID, trin, replika, forsøgsnummer, ind- og udgående kanter og trinnets grænser). S3-legitimation og certifikater til signal-hub'en kommer som separate miljøvariabler fra Secrets. Efter `Finish` kører agenten videre, til den får SIGTERM, som en native sidecar skal. |

#### Filer i stedet for batches

| ID | Niveau | Krav |
|---|---|---|
| GO-101 | SKAL | Alpheus fortolker aldrig indholdet af en chunk. Ingen pakke i Alpheus afhænger af Parquet eller Arrow, og CI kontrollerer med `go list -deps ./...`, at `github.com/apache/arrow-go` ikke er en afhængighed. |
| GO-102 | SKAL | Data mellem agent og komponent går som filer i pod'ens scratch-mappe. Unix-socketten bærer kun kontrolkald. |
| GO-103 | SKAL | Agenten henter en input-chunk fra S3 til scratch-mappen, før `Next` returnerer dens sti, og sletter filen efter `Done`. Output-chunks uploades efter `Close` og committes efter GO-90 og GO-91. |
| GO-104 | SKAL | En output-chunk er én fil fra `Create` til `Close`. Komponenten angiver filendelsen ved `Create` (små bogstaver og tal, højst 16 tegn). Den gemmes i manifestet som `ext` og i objektnavnet `part-000017.<ext>`, men agenten fortolker den ikke. |
| GO-105 | SKAL | `spec.chunks.maxBytes` er en øvre grænse. `Close` på en fil, der er større, giver en fejl med filens størrelse og grænsen. |
| GO-106 | SKAL | `spec.steps[].scratch` (standard 10 GiB) bliver `sizeLimit` på scratch-mappens `emptyDir`. Agenten afviser `Next` og `Create` med en tydelig fejl, før mappen bliver fuld, så pod'en ikke bliver evicted. |
| GO-107 | SKAL | Agenten streamer mellem S3 og disk med en fast buffer, så dens hukommelsesforbrug ikke afhænger af chunk-størrelsen. Erstatter GO-27. |

Dette ændrer den funktionelle specifikations KK-1: komponenter ser lokale
stier i deres egen scratch-mappe, men aldrig S3, beskeder eller andre
pods' filer.

#### Mange indgange

| ID | Niveau | Krav |
|---|---|---|
| GO-108 | SKAL | Hver indgående kant ender i en navngivet indgang på trinnet (`edges[].port`, standard `default`). `Next` returnerer for hver chunk: indgang, producerende trin, producent-replika, chunk-nummer, lokal sti, `ext` og `bytes`. |
| GO-109 | SKAL | `Next` kan kaldes uden indgang (næste chunk fra en hvilken som helst indgang) eller med en indgang (kun fra den). En komponent må blande de to, fx læse en lille opslagstabel helt først og derefter streame en stor indgang. |
| GO-110 | SKAL | En komponent må have op til `spec.steps[].maxOpen` input-chunks åbne ad gangen pr. replika (standard 8). Inden for en strøm afsluttes chunks med `Done` i rækkefølge (GO-71); på tværs af strømme er der ingen krav. `Next` blokerer, når grænsen er nået. |
| GO-111 | SKAL | Agenten henter op til `spec.steps[].prefetch` chunks pr. strøm ned i forvejen (standard 2), inden for scratch-grænsen (GO-106). |
| GO-112 | SKAL | `spec.steps[].inputOrder` er `arrival` (standard) eller `deterministic`. Med `arrival` leverer `Next` uden indgang chunks i den rækkefølge, de bliver klar. Med `deterministic` leverer den skiftevis én chunk fra hver strøm, sorteret efter indgang, trin og replika, og springer strømme over, der har nået EOS. Det giver samme rækkefølge i hver kørsel på bekostning af ventetid. |

### 7.3 Chunks og manifest (`internal/chunk`)

En kant hedder `<fra>-<til>`, eller `<fra>.<udgang>-<til>` for en anden
udgang end `default`. Hver producent-replika skriver sin egen strøm på
kanten, i mappen `runs/<uid>/<kant>/<replika>/` (replika 0 for et trin
uden `replicas`, GO-83). Chunks i en strøm nummereres fra 1 og uden huller.

S3 har ingen rename. Derfor er manifestet commit-punktet i stedet for en
omdøbning:

1. Producenten skriver chunk-objektet direkte under sit endelige navn
   `runs/<uid>/<kant>/<replika>/part-000017.<ext>` (GO-104).
2. Producenten skriver derefter manifestet
   `runs/<uid>/<kant>/<replika>/manifest.json` med chunken tilføjet.
3. En chunk findes kun logisk, når den står i manifestet. Forbrugere
   læser aldrig objekter, der ikke er listet.
4. Ved genstart sletter producenten objekter, der ikke står i
   manifestet (forældreløse chunks fra et nedbrud midt i en skrivning).

Det erstatter "midlertidigt navn og omdøbning" i den funktionelle
specifikations CH-1, som bør rettes til denne formulering.

Manifestformat (version 1):

```json
{
  "version": 1,
  "run": "sales-dk-0042",
  "uid": "3f0c9a6e-…",
  "edge": "read-filter",
  "replica": 0,
  "epoch": 2,
  "segments": ["manifest-0000.json"],
  "chunks": [
    {"n": 10017, "ext": "csv", "bytes": 1832211, "sha256": "…",
     "counts": {"rows": 50000},
     "src": {"edge": "source-read", "replica": 0, "n": 4711}}
  ],
  "consumed": {"source-read/0": 4711},
  "eos": false,
  "last": null,
  "observed": {"seq": 4711, "counts": {"rows": 235550000}, "errors": 0,
               "warnings": 2, "errorsDropped": 0, "segments": 1}
}
```

- `epoch` er forsøgsnummeret (GO-67) for den agent, der skrev manifestet.
- `ext` er filendelsen, komponenten angav ved `Create` (GO-104).
- `counts` er komponentens committede tællere for chunken (GO-115).
- `segments` lister forseglede manifest-segmenter (GO-32). `chunks`
  indeholder kun chunks, der endnu ikke er forseglet.
- `src` angiver den input-chunk, en output-chunk blev lavet af. Kilder
  (trin uden indgående kanter) udelader feltet.
- `replica` er nummeret på den producent-replika, der ejer strømmen.
- `observed` er producent-replikaens samlede, committede tællere og
  fejllog-totaler (GO-115, GO-118).
- `last` er nummeret på strømmens sidste chunk, sat sammen med `eos`. Før
  EOS er det `null`, og for en strøm, der sluttede uden chunks, er det 0.
- `consumed` angiver pr. indgående strøm (`<kant>/<replika>`) den højeste
  input-chunk, hvis output til denne strøm er fuldt committet, også når
  den ikke gav noget output (se 7.4). En replika ser kun på de chunks,
  der er tildelt den (GO-76).

| ID | Niveau | Krav |
|---|---|---|
| GO-30 | SKAL | Hver strøm har præcis én skriver af manifestet (agenten for den producent-replika, der ejer strømmen), og hver ack-fil har præcis én skriver (forbruger-replikaens agent, GO-36). Derfor kræves ingen låsning eller betingede skrivninger i S3. Enkeltskriveren sikres af GO-67. Den eneste undtagelse er controllerens lukning af en fejlet replikas strømme (GO-97), som sker efter bekræftet stop og med højere `epoch`. |
| GO-31 | SKAL | Checksum (SHA-256) beregnes under skrivning og verificeres under læsning. |
| GO-32 | SKAL | Når `chunks` i `manifest.json` når 1.000 chunks, skrives de til et forseglet, uforanderligt segment (`manifest-0000.json`, `manifest-0001.json` osv.). Derefter skrives `manifest.json` med segmentet tilføjet til `segments` og en tom `chunks`. Segmentet skrives altid før `manifest.json`. Så vokser den fil, der omskrives ved hver chunk, ikke ubegrænset. Grænsen er lav, fordi manifestet omskrives ved hvert commit, så prisen for et commit vokser med den. |
| GO-33 | — | Udgået: Parquet er ude af Alpheus (GO-101). |
| GO-68 | SKAL | Agenten afviser et manifest med lavere `epoch` end sit eget forsøgsnummer, og et manifest med højere `epoch` end sit eget får agenten til at stoppe straks. Begge dele sætter trinnet til `Failed` med årsagen "fencing". Brud på GO-67 bliver dermed opdaget i stedet for at give stille datatab. |
| GO-69 | SKAL | Lageret skal have stærk read-after-write-konsistens for nye objekter, overskrivninger og listning. Det gælder AWS S3, Garage og MinIO. Installationsdokumentationen nævner kravet, og agenten advarer ved opstart, hvis lageret ikke er på listen over testede lagre. |
| GO-136 | SKAL | Er lageret nede eller svarer det med en midlertidig fejl (en afvist eller afbrudt forbindelse, 5xx, 429 eller 408), prøver agenten igen med eksponentiel backoff (0,5 til 15 sekunder) i op til `spec.storage.timeout` (standard `10m`, højst `24h`, uforanderlig) fra første fejl, uden at bruge en genstart. Komponenten mærker det som et langsomt kald, som ved backpressure. En læsning, der afbrydes midt i et objekt, fortsætter, hvor den slap, men kun fra samme version (ETag), så en læser aldrig får begyndelsen af én version og slutningen af en anden. En skrivning sendes igen; det er sikkert, fordi hver nøgle har én skriver (GO-30). Fejl, der siger, at forespørgslen er forkert, fx en afvist nøgle, en manglende bucket eller en fuld kvote, prøves ikke igen. Svarer lageret ikke inden for `timeout`, stopper agenten med årsagen "storage unreachable", som controlleren viser i trinnets status, og forsøget tæller som fejlet efter de almindelige regler (GO-67). Controlleren og CLI'en prøver ikke igen på denne måde: controlleren forsøger igen ved næste reconcile. |
| GO-137 | SKAL | Afviser lageret runnets nøgler eller bucket (en ukendt nøgle, en forkert hemmelighed, ingen adgang eller ingen bucket), sætter controlleren runnet til `Failed` med det samme med årsagen "the storage refused the keys in Secret <navn>", uden at bruge genstarter, og sletter pod'erne. Det samme gælder oprydningen (GO-20): kan nøglerne ikke slette runnets data, logger controlleren, at data bliver liggende, og fjerner sine finalizers, så runnet og dets namespace kan slettes. |
| GO-138 | SKAL | Et run, der ikke lykkes, har en årsag som en kode, der er en del af API'et, i `status.reason` og i `Ready`-betingelsens reason, ud over beskeden i ord: `InvalidSpec`, `Cancelled`, `ComponentFailed`, `AgentFailed`, `StorageRefused`, `StorageUnreachable`, `StorageFull`, `StorageError`, `ScratchFull`, `PodDeleted`, `PodFailed` og `FailedReplicas`. Hver replika har årsagen til sit sidste fejlede forsøg i `status.steps[].replicas[].reason`, og `status.failedStep` og `failedReplica` er den replika, hvis fejl stoppede runnet. Agenten sætter sin årsag foran sin termination message; er agenten fejlet, er dens årsag replikaens, da komponenten så kun fik SIGTERM. |
| GO-139 | SKAL | En komponent, der fejler, siger hvorfor med egne ord: SDK'et i Go og Python-modulet skriver fejlen til `/dev/termination-log`, og komponent-containeren har `terminationMessagePolicy: FallbackToLogsOnError`, så slutningen af loggen bruges, når intet er skrevet. Controlleren skriver Kubernetes-events (events.k8s.io) på runnet, når det starter, ved hvert fejlet forsøg og genstart, når en replika giver op, og når runnet slutter, med årsagerne fra GO-138. Events og metrics skrives først, når statussen er skrevet, så et gentaget pas ikke tæller to gange. |
| GO-140 | SKAL | Før controlleren erstatter eller sletter et fejlet forsøgs pod, gemmer den slutningen af hver containers log, højst 4 MiB, under `results/<run>/<uid>/_logs/<trin>/<replika>/attempt-<n>/` (`component.log`, `agent.log` og `agent-previous.log`, når agenten genstartede på stedet), med samme TTL som resultater. `status.steps[].logs` siger hvor, og `alpheus logs --kept` viser dem. Det er best effort og stopper aldrig runnet. |
| GO-141 | SKAL | Et fejlet forsøg gemmes, så det kan køres igen for sig selv: `results/<run>/<uid>/_failures/<trin>/<replika>/attempt-<n>/failure.json` med årsag, besked, image (låst med digest), settings og input-chunks, og kopier af de input-chunks, forsøget havde fået og ikke var færdigt med, under `input/`: de første `maxOpen` i hver strøm, højst 16. Lageret kopierer dem selv (server-side copy). Fejl i lageret gemmer intet. `alpheus replay` henter dem, kontrollerer hver mod sin SHA-256 og kører samme image på dem i local mode med podman eller docker, som replika 0 af 1, uden netværk og med `ALPHEUS_LOG_LEVEL=debug`; `--with none` henter kun. SDK'erne sætter log-niveauet fra `ALPHEUS_LOG_LEVEL`, og Go-SDK'et tager flere porte i `ALPHEUS_LOCAL_INPUTS`. |
| GO-142 | SKAL | `alpheus ui` serverer en side på brugerens maskine (standard `127.0.0.1:7171`) med brugerens kubeconfig, uden noget installeret i klyngen: runs, ét run med flow, replikaer, resultater og events, og én replika med tællere, fejllog, logs (live eller gemte) og fejlede forsøg med en knap, der kører `alpheus replay`. Den opdateres live med server-sent events. Den er skrivebeskyttet, bortset fra replay, svarer kun på forespørgsler til localhost (mod DNS rebinding), og replay kræver et token, som kun siden har. Siden er indlejret i CLI'en, i Go og ren HTML, CSS og JavaScript uden afhængigheder. |
| GO-143 | SKAL | Chartet kan køre `alpheus ui --in-cluster` som en Deployment (`ui.enabled`, standard fra), med CLI'ens binær i sit eget image, bygget, signeret og udgivet med de andre. Den viser runs og events i alle namespaces; logs, fejllog og fejlede forsøg kun i `ui.namespaces`, hvor dens service account må læse Secrets og pod-logs (`["*"]` er alle). Replay-knappen kører `alpheus replay --with cluster`: en pod i runnets namespace, ejet af runnet, hvor agentens image (`-fetch-failure`) henter og kontrollerer chunks, og trinnets image kører på dem i local mode med trinnets ressourcer, `envFrom` og scratch-størrelse; podden slettes bagefter. `ui.replay: false` slår knappen fra og fjerner retten til at oprette pods. Siden svarer kun på forespørgsler til localhost (gennem `kubectl port-forward`), Ingress'ens vært og værterne i `ui.hosts` (`--host`), og den har ingen login: en Ingress eller en NodePort (`ui.service`) skal have en proxy, der autentificerer, foran, uden for testklyngen. I testklyngen kører UI'et altid efter `make deploy` eller `make e2e` på VM'ens port 30717. Siden starter lyst og har et mørkt cyberpunk-look, som brugeren slår til; browseren husker valget. |
| GO-34 | BØR | Lokale kopier af chunks i scratch-mappen slettes, så snart de er ackede eller skrevet. |

### 7.4 Backpressure og genstart

For et trin med udgående kanter er det de udgående manifester, der
er commit-punktet for en input-chunk, ikke ack-filen. Når en input-chunk
er behandlet, gør agenten følgende:

1. Skriver de resterende output-chunks.
2. Opdaterer hvert udgående manifest med chunkene og sætter
   `consumed[<indgående kant>/<replika>]` til input-chunkens nummer.
3. Skriver acken i ack-filen.

Et nedbrud mellem trin 2 og 3 giver derfor ikke dubleret output.

| ID | Niveau | Krav |
|---|---|---|
| GO-35 | SKAL | `maxUnacked` gælder pr. forbruger-replika, ikke for strømmen samlet. Producentens `Emit` blokerer, når den næste chunk er tildelt en forbruger-replika (GO-76), der allerede har `maxUnacked` u-ackede chunks fra strømmen, hos mindst ét forbrugende trin. Med 10 replikaer og `maxUnacked: 4` kan op til 40 chunks fra strømmen være i luften. Komponenten mærker backpressure som et langsomt kald, ikke som en fejl. Kun committede chunks tæller som u-ackede: chunks fra en input-chunk, der endnu ikke er `Done`, kan forbrugeren hverken se eller acke, så et trin, der laver `maxUnacked` eller flere chunks til samme forbruger-replika fra én input-chunk, ville ellers gå i stå. Sådan et trin kan derfor kortvarigt have flere end `maxUnacked` u-ackede chunks. |
| GO-94 | SKAL | Fordi chunks skrives i rækkefølge, og fordelingen er fast, venter producenten på den replika, næste chunk er tildelt, selv om andre replikaer er ledige. Den langsomste replika sætter derfor tempoet for strømmen. Det er et bevidst valg, der giver deterministisk fordeling og simpel genstart, og dokumentationen nævner det. |
| GO-36 | SKAL | Hver forbruger-replika har sin egen ack-fil pr. strøm, `runs/<uid>/<kant>/<replika>/acks-<trin>-<forbruger-replika>.json`. Ved fan-out og replikaer har en strøm derfor flere ack-filer, men hver fil har kun én skriver. Filen lister numrene på de ackede chunks og forfatterens forsøgsnummer: `{"version": 1, "acked": [1, 11, 21], "epoch": 2}`. Agenten læser filen før hver skrivning og stopper ved et højere `epoch` eller `closed: true` (GO-68, GO-97). For et trin uden udgående manifester har filen også `observed` (GO-115). |
| GO-37 | SKAL | En input-chunk på en indgående strøm er behandlet, når alle udgående manifester har `consumed[<kant>/<replika>]` større end eller lig med dens nummer. Ved genstart bruger agenten dette, ikke ack-filen, når der er udgående kanter. Den opdaterer ack-filen, så den passer, og fortsætter fra første ubehandlede chunk. |
| GO-70 | SKAL | Ved genstart sletter agenten output-chunks, der ikke står i manifestet (7.3 trin 4). Hvis en input-chunk genbehandles og et udgående manifest allerede har committet den (`consumed` ≥ nummeret), kasserer agenten komponentens output til netop den kant. Det giver exactly-once pr. kant, også når komponenten ikke er deterministisk. |
| GO-71 | SKAL | Input-chunks på samme indgående kant afsluttes (`Done`) i rækkefølge. Agenten afviser `Done` i forkert rækkefølge. |
| GO-80 | SKAL | En forbruger-replika har fået EOS, når hver indgående strøm har `eos` og alle chunks, der er tildelt replikaen, er behandlet. Først da kaldes komponentens `OnEOS` (GO-48). Derefter skriver agenten `eos` i alle replikaens udgående manifester, også når replikaen ikke har lavet noget output; så har manifestet ingen chunks, men `eos: true`. Ellers venter forbrugerne for evigt. |
| GO-90 | SKAL | For et trin med indgående kanter lukker `Close` kun chunken; den committes ikke. Alle output-chunks fra en input-chunk skrives i manifestet samtidig med `consumed`, når input-chunken er `Done` (7.4 trin 2). Et nedbrud midt i en input-chunk giver derfor aldrig synligt, dubleret output. |
| GO-91 | SKAL | En kilde er et trin uden indgående kanter. For en kilde er hver lukket output-chunk et commit-punkt: den skrives i manifestet, så snart den er lukket. Når komponenten kalder `Finish` (GO-125), skriver agenten `eos: true`. `sdk.Run` kalder `Finish`, når handleren returnerer `nil`. |
| GO-92 | SKAL | En kilde skal være deterministisk: samme settings giver de samme chunks i samme rækkefølge. Ved genstart læser agenten manifestet, og hvis det har *k* chunks, kasserer den de første *k* chunks, komponenten lukker, og committer først fra chunk *k*+1. Agenten sammenligner checksum (GO-31) for de kasserede chunks med manifestet. Ved forskel sættes trinnet til `Failed` med årsagen "kilden er ikke deterministisk". |
| GO-93 | SKAL | Kilder, der læser ekstern tilstand (fx et S3-prefix eller et share), er ikke deterministiske og understøttes ikke i v0.1.0. En senere version giver dem en genoptagelsesposition via SDK'et. Dokumentationen siger det tydeligt. |
| GO-95 | SKAL | Et trin, hvis komponent har registreret `OnEOS` (GO-48), er tilstandsfuldt: dets tilstand findes kun i komponentens hukommelse. For et sådant trin flytter agenten ikke `consumed` i de udgående manifester, før `OnEOS`-outputtet er skrevet. Chunks fra `OnEOS`, `consumed` for alle indgående strømme og `eos: true` committes i samme skrivning af hvert udgående manifest. Ved genstart før denne skrivning behandles alle tildelte input-chunks forfra, så tilstanden genopbygges. |
| GO-96 | SKAL | Et tilstandsfuldt trin acker stadig sine input-chunks løbende (GO-36), så producenterne ikke blokeres af backpressure. En ack er derfor ikke en garanti for, at chunken er committet. Input-chunks må først slettes fra lageret, når alle forbrugende replikaer har dem i `consumed`, ikke når de er ackede. v0.1.0 sletter ingen chunks under et run: et run kræver lager til hele sin mellemliggende datamængde, indtil `runs/<uid>/` slettes efter `ttlAfterFinished` (6.1). `maxUnacked` begrænser arbejdet undervejs, ikke lagerforbruget. |
| GO-72 | SKAL | Resultater (5.2) tæller som udgående manifester, så et trin med resultater får exactly-once som ved kanter. For et trin uden udgående kanter og uden resultater er ack-filen commit-punktet. Effekter uden for Alpheus (fx skrivning til en database) er at-least-once, og dokumentationen for komponent-SDK'et siger det tydeligt. |


## 8. Signal-drivere (`internal/signal`)

```go
type Driver interface {
    Publish(ctx context.Context, sig Signal) error
    Subscribe(ctx context.Context, run, edge string) (<-chan Signal, error)
    Close() error
}
```

`builtin` er den eneste driver. Interfacet og feltet `signals.driver` i
CRD'en, der kun har værdien `builtin`, er beholdt som et bevidst
snitpunkt, så en anden driver kan tilføjes uden at ændre agenten eller
API'et. De bruges ikke til andet.

| ID | Niveau | Krav |
|---|---|---|
| GO-38 | SKAL | `builtin`: agenter holder én bidirektionel gRPC-stream åben til controllerens hub (`proto/signal.proto`). Hub'en videresender signaler pr. run og kant og gemmer intet varigt. |
| GO-73 | SKAL | Hub'en kører kun i den replica, der er leder (GO-21). Lederen sætter label'en `alpheus-dev.org/signal-hub=active` på sin egen pod og fjerner den, når den mister lederskabet. Hub-Service'en vælger pods med denne label. Ved failover genforbinder agenter med eksponentiel backoff og poller i mellemtiden (GO-40). |
| GO-39 | — | Udgået: STOMP-driveren er taget ud af planen. |
| GO-40 | SKAL | Driveren falder tilbage til at polle manifestet (standard hvert 5. sekund), hvis forbindelsen er nede. Korrektheden afhænger aldrig af, at et signal kommer frem. |
| GO-41 | SKAL | Forbindelsen mellem agent og hub bruger mTLS med et certifikat pr. forsøg, som controlleren udsteder, når den opretter forsøgets pod, i Secret'en `<pod>-signal`. Certifikatet har en URI-SAN `alpheus://agent/<namespace>/<run>/<uid>/<trin>/<replika>/<forsøg>`. Hub'en tjekker certifikatet ved hver tilmelding og hvert signal: agenten må kun bruge sit eget run, som signaler navngiver med navn og UID (`<run>/<uid>`), runnets UID skal passe, og forsøget må ikke være ældre end replikaens aktuelle (GO-67). Ellers lukkes forbindelsen. Signaler deles pr. namespace, så runs med samme navn i forskellige namespaces ikke ser hinandens. CA'en ligger i Secret'en `alpheus-signal-ca` i `alpheus-system`. Den oprettes af den første leder og læses af alle replicas, så certifikater stadig gælder efter failover. |
| GO-42 | — | Udgået: med én driver er der ingen fælles suite. |


## 9. CLI (`cmd/alpheus`)

| Kommando | Formål |
|---|---|
| `alpheus run -f flow.yaml` | Opret en StreamRun fra en fil |
| `alpheus get [run]` | Vis runs eller ét runs status |
| `alpheus watch <run>` | Følg status live, med lag pr. kant |
| `alpheus logs <run> [trin]` | Vis logs fra trinnenes containere |
| `alpheus cancel <run>` | Sæt `spec.cancel` |
| `alpheus chunks <run> <kant>` | Vis manifestet for en kant |
| `alpheus validate -f flow.yaml` | Valider en flow-fil uden at oprette den |
| `alpheus results gc [--dry-run]` | Find og slet resultater med udløbet TTL eller uden StreamRun (GO-99) |
| `alpheus version` | Vis CLI-, controller- og CRD-version |

CLI'en er skrevet med standardbibliotekets `flag` i `internal/cli`, så den
kan testes mod envtest (`make test`), og bygges med `make cli`. Flag kan
stå før og efter argumenterne. Alle kommandoer har `--kubeconfig`,
`--context`, `-n` og `-o`. Det, der er værd at vide om de enkelte:

- `run` og `validate` læser filen strengt, så et felt med en stavefejl
  er en fejl, og kører controllerens egne tjek af grafen før API-serveren.
  Et image uden digest giver en advarsel, fordi controlleren afviser det
  uden `-allow-tags`. `validate` lader API-serveren tjekke CRD'ens skema
  og CEL-regler med en dry-run; `--local` springer det over.
- `watch` udskriver hele status ved hver ændring (med `-o json` ét objekt
  pr. linje) og slutter, når runnet er færdigt, med exit-kode 0 kun ved
  `Succeeded`, så scripts kan vente på et run med den.
- `logs` viser komponenten eller med `-c agent` agenten, for alle trin
  eller ét, med `--follow` og `--tail`.
- `chunks` læser manifesterne for en kant eller et resultat
  (`<trin>.<udgang>`) med nøglerne i runnets storage-Secret. Er
  endpointet kun nåeligt i klyngen, angives et andet med `--endpoint`.
- `results gc` får lageret med flag (`--endpoint`, `--bucket`,
  `--secret` m.fl.), da StreamRun-objektet kan være væk.
- `version` viser CLI'ens version, som Go stempler i binæren, og
  controllerens og CRD'ens, hvis klyngen kan læses.

| ID | Niveau | Krav |
|---|---|---|
| GO-43 | SKAL | CLI'en bruger brugerens kubeconfig og har ingen egen server eller egen login. |
| GO-44 | SKAL | Alle kommandoer understøtter `-o json` og `-o yaml`, så de kan bruges i scripts. |
| GO-45 | SKAL | Fejl giver en exit-kode ≠ 0 og en besked, der siger, hvad der gik galt, og hvad man kan gøre. |


## 10. Komponent-SDK (`sdk/`)

En komponent læser input-chunks som filer og skriver output-chunks som
filer. Formatet er komponentens eget; Alpheus læser det ikke:

```go
func main() {
    sdk.Run(func(ctx context.Context, in sdk.Input, out sdk.Output) error {
        for chunk, err := range in.Chunks(ctx) {
            if err != nil {
                return err
            }
            dst, err := out.Create(ctx, "default", "csv")
            if err != nil {
                return err
            }
            if err := filter(chunk.Path, dst); err != nil {
                return err
            }
            if err := dst.Close(); err != nil {
                return err
            }
        }
        return nil
    })
}
```

`in.Port("lookup").Chunks(ctx)` læser kun fra én indgang (GO-109).

| ID | Niveau | Krav |
|---|---|---|
| GO-46 | SKAL | `sdk.Run` håndterer socket-forbindelse, `Next`/`Done`-løkken, signaler for nedlukning og exit-koder. Komponentens kode ser kun filer i sin egen scratch-mappe og aldrig S3. |
| GO-47 | SKAL | SDK'et har en lokal testmode, der læser input-filer fra disk og skriver til disk uden agent, så komponenter kan testes uden Kubernetes. |
| GO-48 | SKAL | Komponenter med tilstand på tværs af chunks får et `in.OnEOS`-hook til at skrive deres resultat til sidst. |
| GO-49 | BØR | Samme komponent-binær kan køre i MCPipes batch-mode på Argo ved at SDK'et genkender miljøet og læser én stor chunk. |
| GO-86 | SKAL | En output-chunk lukkes, når komponenten kalder `Close` på den, eller automatisk, når den input-chunk, den blev lavet af, er færdig (`Done`). `Flush` og `maxRows` fjernes fra komponent-API'et og fra StreamRun. |
| GO-87 | SKAL | Lokal testmode vælges med `ALPHEUS_MODE=local` og mapperne `ALPHEUS_LOCAL_INPUT` (alle filer undtagen `manifest.json` og dets segmenter, læst i navneorden, så et trins udgangsmappe kan være næste trins input) og `ALPHEUS_LOCAL_OUTPUT` (én mappe pr. udgang, med manifest-formatet fra 7.3, og filerne `_summary.json` med tællere og antal fejl og `_errors.jsonl` med fejlloggen). `ALPHEUS_SETTINGS` er trinnets settings som JSON, og `ALPHEUS_REPLICA` og `ALPHEUS_REPLICAS` vælger, hvilke chunks replikaen læser (GO-76). Komponenttests kalder `sdk.RunLocal` direkte. |


### 10.1 Tællere, fejl og fremdrift

Komponenter kan rapportere, hvor meget de har behandlet, i en enhed de
selv vælger, og skrive fejl til en fejllog, der kun tilføjes til. Alpheus
tæller og gemmer, men fortolker ikke, hvad der tælles.

```go
sdk.Count(ctx, "rows", 50000)
sdk.Count(ctx, "files", 3)
sdk.Error(ctx, "missing_amount", "amount is empty", sdk.Field("row", 9))
sdk.Warn(ctx, "date_format", "date parsed as DD-MM-YYYY", sdk.Field("row", 14))
sdk.Progress(ctx, "4,2 mio. rækker behandlet")
```

**Tællere**

| ID | Niveau | Krav |
|---|---|---|
| GO-113 | SKAL | `Observe` er en strøm fra komponent til agent med tællere, fejl, advarsler og fremdrift. Den erstatter `Report`. |
| GO-114 | SKAL | En tæller har en enhed, komponenten selv vælger (små bogstaver, tal og `_`, højst 32 tegn), og et antal. Alpheus fortolker ikke enheden. Et trin kan højst have 16 forskellige enheder; flere giver en fejl ved `Observe`. |
| GO-115 | SKAL | Tællere, der rapporteres, mens en input-chunk behandles, committes, når den er `Done`; for en kilde med den output-chunk, der lukkes, som også får dem i sin manifest-post (`counts`). Replikaens samlede tællere og fejllog-totaler står i feltet `observed` med et løbenummer `seq`, i samme skrivning som `consumed`, i hvert udgående manifest, commit'et ændrer. Et trin uden udgående manifester og resultater har `observed` i sine ack-filer, der er dets commit-punkt (GO-72). Kopien med højest `seq` er replikaens committede tilstand; en genstartet agent fortsætter fra den. Et tilstandsfuldt trin holder sine tællere tilbage til `OnEOS`-outputtet committes (GO-95), og en chunk, en genstartet kilde kasserer (GO-92), committer intet. `status.steps[].counts` er summen over replikaerne, så tallene er præcise, også efter en genstart. |
| GO-116 | SKAL | Agenten eksponerer tællerne live på `/metrics` som `alpheus_component_count_total` med labels for run, trin, replika og enhed. Live-tallene er best-effort og nulstilles ved genstart; status er det præcise tal. |

**Fejllog**

| ID | Niveau | Krav |
|---|---|---|
| GO-117 | SKAL | Fejl og advarsler har niveau (`error` eller `warn`), kode, besked og valgfrie felter. Agenten skriver dem som JSON Lines med tid, trin, replika og input-chunk tilføjet. |
| GO-118 | SKAL | Fejlloggen er en del af chunk-commit'et: poster fra en input-chunk skrives som et nyt segment, når chunken er `Done`, før commit'et, og `observed.segments` i commit'et siger, hvor mange segmenter der er committet. En genstartet agent sletter segmenter ud over det, som forældreløse chunks, så poster aldrig står to gange og aldrig mangler. Loggen skrives som nummererede segmenter (`log-000001.jsonl` …) med et resumé (`manifest.json`) under `results/<run>/<uid>/_errors/<trin>/<replika>/` og har samme TTL som resultater (GO-78). Resuméet skrives efter commit'et og er kun en kopi: en genstartet agent skriver det igen fra `observed`, og controlleren bruger `observed`, hvis det er nyere. Trinnavne kan ikke begynde med `_` (CRD'ens mønster), så stier med `_` er forbeholdt Alpheus og støder aldrig sammen med et trins resultater. |
| GO-119 | SKAL | `status.steps[].errors` og `warnings` viser antallet, og `errorLog` viser stien. `alpheus get` og `alpheus watch` viser dem. |
| GO-120 | SKAL | En post er højst 4 KB, og en replika kan højst skrive 100.000 poster. Poster ud over grænserne gemmes ikke, men tælles i `errorsDropped`. Dokumentationen anbefaler at henvise til rækkenummer og felt frem for indhold, da fejlbeskeder ellers let kommer til at indeholde persondata. |

**Måling og opsummering**

| ID | Niveau | Krav |
|---|---|---|
| GO-121 | SKAL | Agenten måler selv tiden fra `Next` til `Done` for hver input-chunk og eksponerer den som histogrammet `alpheus_chunk_duration_seconds`. Komponenten skal ikke gøre noget. |
| GO-122 | BØR | Fremdrift er en kort tekst, der vises i `status.steps[].replicas[].message` og i `alpheus watch`, højst én opdatering i sekundet pr. replika (GO-22). Den er best-effort og committes ikke. |
| GO-123 | SKAL | Når runnet er afsluttet, skriver controlleren `results/<run>/<uid>/summary.json` med fase, tællere, antal fejl og advarsler pr. trin og replika. Den overlever, at StreamRun-objektet slettes, og har samme TTL som resultater. |
| GO-124 | SKAL | `Observe` blokerer aldrig komponenten. Tællere og fejl, der skal committes, holdes af agenten indtil `Done`; kun live-metrics og fremdrift kan smides væk, hvis agenten er travl. |


## 11. Logning, metrics og tracing

| ID | Niveau | Krav |
|---|---|---|
| GO-50 | SKAL | Logs skrives med `log/slog` som JSON til stdout. Faste felter: `run`, `step`, `edge`, `chunk`, `component`. |
| GO-51 | SKAL | Metrics eksponeres på `:9090/metrics`, af controlleren og af hver agent i trin-pod'en (som har `prometheus.io/scrape`-annotationer; en komponent må ikke lytte på 9090). Navne har præfikset `alpheus_`: agenten har `alpheus_chunks_total`, `alpheus_bytes_total`, `alpheus_backpressure_seconds_total`, `alpheus_storage_retries_total`, `alpheus_component_count_total` og `alpheus_component_log_entries_total` med labels run, trin og replika; controlleren har `alpheus_runs_started_total`, `alpheus_runs_finished_total`, `alpheus_step_restarts_total` og `alpheus_edge_lag`. Chartet kan installere en ServiceMonitor, en PodMonitor og et Grafana-dashboard. |
| GO-52 | SKAL | Controller og agent har `/healthz` og `/readyz`. |
| GO-53 | BØR | OpenTelemetry-tracing kan slås til, med et span pr. chunk fra skrivning til ack. |


## 12. Test

| Niveau | Hvad | Hvordan |
|---|---|---|
| Unit | chunk, manifest, backpressure, graf-validering | `go test`, ingen netværk |
| Integration | controller mod API-server | envtest |
| Integration | agent mod S3 | Garage i en container |
| Driver | `builtin` | hub og klient i samme proces, og agenter gennem hub'en mod Garage |
| E2E | testcase01 til testcase15 | Talos i en VM med Vagrant og libvirt (`vagrant/` i repo'ets rod) med Garage og Zot (12.2) |

| ID | Niveau | Krav |
|---|---|---|
| GO-54 | SKAL | Alle accepttests fra den funktionelle specifikation kører automatisk i CI på hver pull request mod `main`. |
| GO-55 | SKAL | Hver testcase kontrollerer sit resultat mod noget, der er beregnet uden Alpheus: kildens egne data eller en kendt værdi. |
| GO-56 | SKAL | En kaos-test slår tilfældige trin-pods ihjel under et run og kontrollerer, at resultatet stadig er korrekt (GO-55). |
| GO-57 | SKAL | `go test -race` kører på alle pakker. |
| GO-58 | BØR | Fuzz-tests for manifest-parsing i agenten. |
| GO-74 | SKAL | Genstartstests slår agenten ihjel mellem hvert trin i commit-rækkefølgen (7.3 og 7.4) og kontrollerer, at output hverken mangler eller er dubleret. |
| GO-100 | SKAL | En genstartstest slår det tilstandsfulde `sink`-trin ihjel, efter at det har acket halvdelen af sine input-chunks, men før EOS. Resultatet skal stadig have hver chunk præcis én gang og uændret, og tællerne skal være præcise. Testen beviser GO-95. |

### 12.1 Testcases

Testkomponenterne ligger i `test/components/` og bygges som images, der
skubbes til Zot i testmiljøet. De bruger et simpelt format fra Gos
standardbibliotek (CSV), da Alpheus ikke kender Parquet (GO-101).

Testcasene er Alpheus' egne. De tester det, Alpheus gør, og kender ikke
noget bestemt dataformat.

**testcase01: måling af systemet.**

```
source ──> pass ──> sink ──> resultat i S3
```

En simpel testcase, der også viser, hvordan systemet yder. Alle tre trin
bruger testkomponenten `bench`, så hver chunk går to gange gennem
lageret.

- `source` laver `chunks` chunks på `size` bytes. Hver chunk begynder
  med sit nummer og tidspunktet, den blev lavet, efterfulgt af bytes fra
  en ChaCha8-generator med et fast seed.
- `pass` kopierer hver chunk uændret til sit output.
- `sink` kontrollerer hver byte mod generatoren og noterer, hvornår
  chunken kom. Ved EOS skriver den resultatet `n,bytes,written_ns,received_ns`
  med én linje pr. chunk, med indgangen først
  (`port,n,bytes,written_ns,received_ns`). En forkert, dubleret eller
  manglende chunk får trinnet til at fejle.

Testen kontrollerer, at runnet ender i `Succeeded`, at resultatet har
alle chunks, og at alle tre trins tællere passer. Den finder resultatet
med `status.results` alene (GO-82). Den måler:

| Måling | Fra |
|---|---|
| Opstart | Runnet er oprettet, til alle komponenter kører (pod-status, hele sekunder) |
| Strøm | Første chunk er lavet, til sidste er modtaget |
| I alt | Runnet er oprettet, til `Succeeded` |
| Gennemløb | Bytes delt med strømmens varighed |
| Latens | Pr. chunk fra den laves, til `sink` har den; p50, p95 og max |

Størrelserne kan ændres med `BENCH_CHUNKS`, `BENCH_SIZE`,
`BENCH_PASS_REPLICAS` og `BENCH_MAX_UNACKED` (standard 64 chunks på
4 MiB, én `pass`-replika og `maxUnacked: 4`). `make bench` kører
testcase01, testcase02 og testcase03 og gemmer målingerne som JSON i `_out/bench/`.
Alle trin kører på samme node, så tiderne fra `source` og `sink` kan
sammenlignes.

**testcase02: fan-out og fan-in.**

```
            ┌──> left  (pass, 3 replikaer) ──┐ indgang left
source ─────┤                                ├──────────────> sink ──> resultat i S3
            └──> right (pass, 2 replikaer) ──┘ indgang right
```

Samme komponent som testcase01, men strømmen deler sig og samles igen.
`source`s ene udgang har to kanter, så hver chunk går til både `left` og
`right`, der hver fordeler chunks mellem sine egne replikaer. `sink` har
to indgange og indstillingen `ports: [left, right]`: den skal have hver
chunk præcis én gang på hver indgang. Replikaantallene er forskellige,
så de to grene fordeler chunks forskelligt.

Testen kontrollerer ud over testcase01's kontroller:

- Fan-out: `source`s strøm har egne ack-filer for `left` og `right`
  (GO-36). `left`s tre filer indeholder tilsammen 1–64 præcis én gang,
  med chunk n hos replika n mod 3, og `right`s to filer det samme med
  n mod 2 (GO-76).
- Fan-in: `consumed` i `sink`s resultat-manifest har alle fem indgående
  strømme, hver til strømmens sidste chunk, og hver strøm har `eos`
  (GO-80, GO-95). Hver chunk kom på sin grens indgang.
- Tællerne: `source` 64, `left` og `right` 64 hver over replikaerne,
  `sink` 128.

Målingerne er de samme som i testcase01, med latens pr. indgang.
Replikaerne ændres med `BENCH_LEFT_REPLICAS` og `BENCH_RIGHT_REPLICAS`.

**testcase03: replikaer og en replika uden output.**

```
source ──> filter (pass, 10 replikaer, drop: 10) ──> sink ──> resultat i S3
```

Samme komponent som testcase01. `pass` har indstillingen `drop`: den
dropper hver chunk, hvis nummer i dens input-strøm går op i `drop`, og
tæller dem i enheden `dropped`. `sink` har den tilsvarende `skip` og
forventer kun de øvrige chunks.

- `filter` kører med `replicas: 10` og `drop: 10`. `source` har én
  replika (p = 0), så chunk n går til replika n mod 10 (GO-76), og
  replika 0 får præcis chunks 10, 20, 30 …. Den dropper dem alle, laver
  derfor intet output og skal alligevel afslutte med EOS (GO-80). Det er
  testens svære kant.
- `sink` kører med `skip: 10`, kontrollerer hver byte og fejler, hvis en
  chunk, der skulle være droppet, kommer frem.
- Replikaantallet ændres med `BENCH_FILTER_REPLICAS`, og `drop` og
  `skip` følger det, så replika 0 altid får præcis de droppede chunks.
- Resultatet ligger i sin egen bucket, `alpheus-results` (GO-129).
  `status.results` peger på Secret'en `alpheus-s3-results` med en nøgle,
  der kun kan læse den. Testen henter resultatet med den (GO-82),
  kontrollerer, at den ikke kan skrive, og at intet af runnets
  resultater ligger i `alpheus-data`.

| ID | Niveau | Krav |
|---|---|---|
| GO-81 | SKAL | Testen kontrollerer, at resultatet har præcis de chunks, hvis nummer ikke går op i `drop`, hver med det indhold, generatoren laver uden Alpheus, og at `filter` talte ⌊chunks/`drop`⌋ som `dropped`. |
| GO-84 | SKAL | Testen læser de ti `filter`-replikaers ack-filer (GO-36) og kontrollerer, at mængderne af ackede chunks er disjunkte, at deres forening er præcis alle chunks, og at chunk n ligger hos replika n mod 10. Testen afhænger ikke af logs. |
| GO-85 | SKAL | Testen kontrollerer, at replika 0's udgående manifest har nul chunks, `last: 0` og `eos: true`, at de andre replikaer skrev én chunk pr. ackede chunk, og at `sink` alligevel er nået til EOS med alle ti strømme forbrugt. |
| GO-82 | SKAL | Testen henter resultatet med oplysningerne i `status.results` og intet andet, så den også tester, at de er nok. |

**testcase04: genstart af hver slags trin.**

```
source (fixed) ──> pass ──> sink (delay: 100)
```

Tre runs, der hver sletter ét trins pod med en grace period på ét
sekund. Agent og komponent stopper sammen på SIGTERM og afbryder en
skrivning, der er i gang, og kubelet dræber resten efter sekundet.
Pod-objektet forsvinder først, når kubelet har bekræftet, at
containerne er stoppet, så det nye forsøg aldrig kører ved siden af
det gamle (GO-67). Controlleren starter derefter et nyt forsøg. Testen vælger tidspunktet ud fra manifester og ack-filer i S3:

| Run | Slås ihjel, når | Beviser |
|---|---|---|
| `source` | Den har committet en fjerdedel af sine chunks | En deterministisk kilde fortsætter fra næste chunk efter at have sammenlignet dem, den laver igen (GO-92) |
| `pass` | Den har committet halvdelen af sit output | Den fortsætter fra første ubehandlede chunk, sletter output, der aldrig blev committet, og intet står to gange (GO-37, GO-70) |
| `sink` | Den har acket halvdelen af sit input, før EOS | Et tilstandsfuldt trin læser alt sit input igen (GO-95, GO-100) |

`source` har indstillingen `fixed`, der skriver tiden 0 i chunkens
header, så hvert forsøg laver de samme chunks; ellers ville agenten med
rette afvise kilden som ikke-deterministisk. `sink` har `delay`, så
testen når at handle. Testen kontrollerer, at runnet lykkes, at kun det
dræbte trin har én genstart, at `sink` fik hver chunk præcis én gang og
uændret, at hver strøm har chunk 1–64 og EOS, at der ikke ligger
chunk-objekter, som aldrig blev committet, og at tællerne er præcise.

**testcase05: backpressure.**

```
source ──> slow (pass, 2 replikaer, delay: 500) ──> sink
```

Mens runnet kører, læser testen ca. fem gange i sekundet `source`s
manifest og derefter `slow`s to ack-filer. For hver replika må antallet
af chunks, der er tildelt den, men ikke acket, aldrig overstige
`maxUnacked` (GO-35). Manifestet læses først, så en prøve kan tælle for
få, aldrig for mange. Hver replika skal også nå grænsen, ellers blev
backpressure ikke testet. Den langsomste replika sætter tempoet (GO-94),
så strømmen tager mindst chunks × delay / replikaer. Replikaerne og
ventetiden ændres med `BENCH_SLOW_REPLICAS` og `BENCH_SLOW_MS`.

**testcase06: kaos.**

testcase02's form med `fixed` kilde og `maxRestarts: 100`. Ca. hvert
tredje sekund slår testen en tilfældig pod i runnet ihjel, i alt
`BENCH_CHAOS_KILLS` gange (standard 12), eller til runnet er færdigt
(GO-56). Så når runnet at blive færdigt: et tilstandsfuldt `sink`, der
slås ihjel med få sekunders mellemrum, ville begynde forfra for evigt. Ud over testcase02's kontroller må der ikke ligge
chunk-objekter, som aldrig blev committet, og tællerne skal være
præcise. `BENCH_CHAOS_SEED` vælger
pod'erne, så en fejl kan gentages; testen logger den.

**testcase07: komponent-skabelonerne.**

```
source ──> go (templates/component-go, 2 replikaer) ──> python (templates/component-python) ──> sink
```

Skabelonerne kører i agent-mode, som brugernes egne komponenter vil gøre.
Begge får et tomt `match`, så hver byte går videre, og `sink` tjekker
chunkene som i testcase01. Kilden er `fixed`, og `sink` venter 100 ms pr.
chunk, så testen når at handle. Testen slår `go`s replika 0 ihjel, når
den har committet en fjerdedel af sine chunks, og `python`, når den har
committet halvdelen. Runnet skal lykkes med hver chunk præcis én gang og
uændret, `go` og `python` skal tælle lige mange linjer ind og ud (præcise
tællere trods genstart, GO-115), hver af dem skal have én genstart, `go`s
to strømme skal tilsammen have alle chunks og EOS, og der må ikke ligge
chunk-objekter, som aldrig blev committet. `hack/images.sh` bygger
Python-skabelonen med dens egen Dockerfile. Antallet af `go`-replikaer
ændres med `BENCH_GO_REPLICAS`.

**testcase08: flagskibet, ETL af store filer.**

```
export ──> split ──> clean (4 replikaer) ──> aggregate ──> totaler (resultat)
                        └──> dataset (resultat)
```

Brugsscenariet, Alpheus bygges og måles omkring: et forsyningsselskabs
natlige eksport af elmåler-aflæsninger kommer som store gzippede CSV-filer
og renses, kontrolleres og summeres på egen klynge uden andet end
Kubernetes og et S3-lager. Komponenterne er `examples/meter-etl` og bruger
kun standardbiblioteket. `export` skriver `ETL_FILES` filer med `ETL_ROWS`
aflæsninger hver ud fra et seed, med 5 dårlige rækker pr. tusind; `split`
skærer dem i chunks på `ETL_SPLIT_BYTES` ukomprimeret; `clean` kontrollerer
hver række, skriver gyldige rækker gzippet til resultatet `dataset`,
logger dårlige rækker i fejlloggen og sender en delsum pr. region og time
videre; `aggregate` er tilstandsfuld og skriver totalerne. Testen beregner
de forventede totaler med samme generator og kræver, at alt er eksakt:
hver række læst én gang, de dårlige afvist og logget, resten i datasettet
(én dataset-chunk pr. split-chunk) og totalerne pr. region og time ens med
generatorens. Medmindre `ETL_KILL=0`, slår den `clean`s replika 0 ihjel,
når `clean` har læst en fjerdedel af rækkerne. Den måler opstart,
behandlingstid (fra alle komponenter kører til `Succeeded`) og
gennemløb, og sletter runnets resultater til sidst, da datasettet er lige
så stort som inputtet. `make bench-etl` (`hack/bench-etl.sh`) kører den
uden drab med 1, 2, 4 og 8 replikaer af `export`, `split` og `clean` og
større data (16 filer à 2 millioner aflæsninger) og skriver en tabel med noden, den kørte på.

**testcase09: lageret går ned (GO-136).** `source -> pass -> sink` som
testcase04, med et `sink`, der venter et sekund pr. chunk. Når `sink` har
acket en fjerdedel af sit input, skalerer testen Garage til 0 og tilbage
igen, én gang efter 20 sekunder og én gang efter 3 minutter
(`storage.timeout: 5m`). Runnet må ikke være færdigt, mens lageret er væk,
og skal derefter slutte som `Succeeded` med hver chunk præcis én gang,
præcise tællere og ingen genstarter.

**testcase10: controlleren dør.** testcase02's fan-out og fan-in med et
langsomt `sink`. Testen sletter controllerens pod to gange, når `sink`
har acket en fjerdedel og tre fjerdedele af sit input fra `left`s første
replika. Signal-hub'en ligger i controlleren og forsvinder med den;
agenterne poller manifesterne i mellemtiden (GO-40). Runnet skal slutte
som `Succeeded` med præcise tællere, acks og `consumed` som i testcase02,
ingen chunks, der aldrig blev committet, og ingen genstarter. En tråd
læser status fire gange i sekundet, og en tæller må aldrig falde, og et
færdigt trin eller run må aldrig blive ufærdigt igen.

**testcase11: lageret afviser nøglerne (GO-137).** Runnet peger på en
Secret med det rigtige nøgle-ID og en forkert hemmelighed, så Garage
svarer `SignatureDoesNotMatch`. Runnet skal være `Failed` inden for to
minutter med årsagen "the storage refused the keys in Secret
alpheus-s3-wrong", uden genstarter og uden pods tilbage, og når det
slettes, skal det forsvinde inden for et minut og slippe sin finalizer på
Secret'en, selv om dets data ikke kan slettes.

**testcase12: bucketen er fuld.** `source -> sink` i bucketen
`alpheus-quota` med en kvote på 20 MiB (`vagrant/services.sh`), langt
mindre end runnet skal bruge, og et langsomt `sink`, så chunks hober sig
op. Runnet skal være `Failed` med en årsag, der nævner kvoten, og ikke
"storage unreachable": en fuld bucket er ikke et udfald (GO-136). Det
committede skal holde: hver chunk i manifestet findes og har sin checksum,
og `source`s tællere svarer til manifestet (GO-115). Testen tømmer
bucketen før og efter.

**testcase13: scratch-mappen er for lille (GO-106).** `sink` har
`scratch: 1Mi` og får chunks på 4 MiB. Agenten skal afvise den første
chunk med en fejl, der nævner scratch-mappen, før mappen bliver fuld og
Kubernetes evicter pod'en; runnet er `Failed` med den årsag, og `sink`
har brugt sin ene genstart.

**testcase14: en komponent med en fejl (GO-138 til GO-141, GO-51).**
`pass` fejler på den chunk, hvis header har nummer 5, i hvert forsøg, og
har én genstart. Mens den arbejder, serverer `sink`s agent sine metrics
(`alpheus_chunks_total`). Runnet skal være `Failed` med årsagen
`ComponentFailed`, `failedStep` `pass`, komponentens egne ord i beskeden,
events for start, genstart, opgivelse og slut, begge forsøgs logs i
bucketen, og det sidste forsøg gemt med den chunk, det fejlede på, som
første input. `alpheus replay --with none` henter inputs, og
bench-komponenten fejler på dem i local mode på samme måde med
debug-linjer i loggen.

**testcase15: UI'et i klyngen (GO-143).** testcase14's komponent med en
fejl, set gennem `alpheus ui`, som `hack/e2e.sh` installerer med chartet
(`ui.namespaces={*}`), nået med `kubectl port-forward`. Siden skal vise
runnet blandt alle namespaces' runs, replikaens gemte forsøg og log, og
replay-knappen skal køre det sidste forsøg som en pod i runnets namespace,
der henter inputs, fejler på samme måde med debug-linjer og slettes
bagefter.

**Agent-test: hvert commit-punkt (GO-74).** En pod, der slås ihjel,
rammer ikke et bestemt sted mellem to skrivninger til S3. Derfor kører
`TestCrashAtEveryWrite` to pipelines i hukommelsen: testcase03's form
(`generate`, `filter` med replikaer, det tilstandsfulde `sum`) og
testcase02's fan-out og fan-in, hvor `generate` skriver hver chunk i to
manifester efter hinanden. For hvert k i et run fejler skrivning nummer
k, både som en skrivning, der aldrig nåede frem, og som en, der nåede
frem, men hvis svar gik tabt. Herefter fejler alle skrivninger, som om
hele runnet døde. Så køres alle replikaer igen som forsøg 2, og
resultatet skal være uden manglende og dublerede data, med præcise
tællere og en fejllog, hvor hver post står præcis én gang (GO-115,
GO-118).

### 12.2 Testmiljø (Vagrant)

`vagrant/deploy.sh` laver en Talos-VM med de samme tjenester som MCPipe
bruger, i de samme versioner: Garage (S3) og Zot (container-registry).
Hver `deploy.sh` laver nye nøgler, og `vagrant/credentials.sh` viser dem.
Bruges Alpheus uden Vagrant, sættes S3-lager og registry op manuelt.

| Tjeneste | Adresse fra værten | Hvad `credentials.sh` viser |
|---|---|---|
| Garage S3 | `https://<VM IP>:30900`, region `garage`, bucket `alpheus-data`, resultat-bucketen `alpheus-results` og `alpheus-quota` med en kvote på 20 MiB (testcase12) | access key, secret key, og nøglen `results-reader`, der kun kan læse `alpheus-results` (GO-129) |
| Garage dashboard | `https://<VM IP>:30901` | admin token (bruges til login) |
| Garage internt | | rpc secret |
| Zot | `https://registry.alpheus.test:30500`, ingen login | Zots egen S3-nøgle til bucket `alpheus-registry` |

HTTPS går gennem en gateway (HAProxy) med et certifikat fra en CA, som
`deploy.sh` laver i `vagrant/_out/`. Talos-noden stoler på CA'en, så den
kan hente images fra Zot.

VM'ens IP kan skifte fra deploy til deploy. Flow-filer bruger derfor det
faste navn `registry.alpheus.test:30500` til images, ikke IP'en. Navnet
står i certifikatet fra CA'en og peger på VM'en via Talos'
statiske værtsnavne (`StaticHostConfig`, tidligere `extraHostEntries`).
På værten skal navnet stå i `/etc/hosts` for at skubbe images;
`credentials.sh` viser linjen.

Zot har ingen login. Det er kun i orden, fordi VM'en ligger på libvirts
host-only/NAT-netværk og ikke kan nås udefra. Det står i
`vagrant/README.md`, så ingen eksponerer den ved et uheld.


## 13. Kodestil og CI

| ID | Niveau | Krav |
|---|---|---|
| GO-59 | SKAL | `gofmt`, `go vet` og `golangci-lint` med en fast konfiguration i repo'et. CI fejler ved fund. |
| GO-60 | SKAL | `context.Context` er første parameter i alle funktioner, der laver I/O, og respekteres for afbrydelse. |
| GO-61 | SKAL | Fejl pakkes med `fmt.Errorf("…: %w", err)`, og sentinel-fejl bruges til tilstande, kalderen skal kunne reagere på. |
| GO-62 | SKAL | Ingen globale mutable variabler, ud over registrering af metrics. |
| GO-63 | SKAL | Genereret kode (CRD'er, protobuf, deepcopy) committes og kontrolleres i CI for at være opdateret. |
| GO-64 | BØR | Offentlige pakker (`api/`, `sdk/`) har godoc-kommentarer på alle eksporterede navne og eksempler. |


## 14. Udgivelse

Alt, hvad en udgivelse består af, ligger på projektets egen infrastruktur
i EU: kildekoden på Forgejo, byggeriet på egne runners, images og chart i
eget registry og filerne på hjemmesiden. Det skyldes Cyber Resilience Act
(CRA): projektet skal selv kunne stå inde for og levere sine udgivelser,
SBOM'er og sikkerhedsopdateringer, uden at de afhænger af tjenester uden
for EU. GitHub er kun et spejl af kildekoden, der læses fra (hjemmesidens
`SPEC.md` WWW-44), og ingen udgivelse lægges der.

1. Tag `vX.Y.Z` på `main` på Forgejo.
2. Forgejo Actions bygger på projektets egne runners: binærer, images,
   SBOM og checksums, og signerer dem.
3. Images skubbes til projektets Zot på `registry.alpheus-dev.org`, som
   alle kan hente fra, men kun CI kan skubbe til. Registryet er en
   indstilling (Helm-værdien `registry` og byggeriets `REGISTRY`): er den
   ikke sat, bruges `registry.alpheus-dev.org`. Er den sat, fx til et
   spejl i en lukket klynge eller Zot i testmiljøet (12.2), hentes og
   skubbes images og chart der i stedet.
4. Helm-chartet udgives som OCI-artefakt samme sted
   (`oci://registry.alpheus-dev.org/alpheus-dev/charts/alpheus`). CI
   skriver agentens og controllerens digests ind i chartets `values.yaml`,
   før det pakkes, så et udgivet chart altid installerer images pinned ved
   digest (GO-24). Images har samme stier i alle registries
   (`alpheus-dev/agent` og `alpheus-dev/controller`), så et spejl kun
   kræver `registry`.
5. Binærer, checksums, SBOM'er, signaturer og release notes lægges under
   `https://alpheus-dev.org/releases/vX.Y.Z/`, og hjemmesiden genbygges
   med dokumentationen for den nye version.

| ID | Niveau | Krav |
|---|---|---|
| GO-65 | SKAL | Hver udgivelse har release notes med ændringer, opgraderingsnoter og eventuelle CRD-ændringer. |
| GO-66 | SKAL | CRD-opgraderinger er bagudkompatible inden for samme API-version. En ny API-version leveres med en conversion webhook. |
| GO-130 | SKAL | Ingen udgivelse, intet image, ingen signatur og ingen SBOM hentes fra eller lægges på tjenester uden for projektets egen infrastruktur i EU. Byggeriet kører på egne runners, ikke på GitHub Actions. |
| GO-131 | SKAL | Signeringsnøglen er projektets egen cosign-nøgle. Den private nøgle findes kun i release-runnerens hemmeligheder, og den offentlige nøgle ligger på `https://alpheus-dev.org/cosign.pub` og i repo'et. Signaturer lægges i registryet ved siden af images og chart og filerne er dækket af den signerede `SHA256SUMS` (GO-133). cosign signerer med en signing config uden tjenester, så der bruges ingen transparency log, tidsstempel-tjeneste eller certifikat-udsteder. |
| GO-132 | SKAL | Hver udgivelse har en SBOM i SPDX-format for hver binær og hvert image, med på `https://alpheus-dev.org/releases/vX.Y.Z/` og vedhæftet hvert image i registryet. |
| GO-133 | SKAL | `https://alpheus-dev.org/releases/vX.Y.Z/` har en `SHA256SUMS`-fil over alle filer i udgivelsen, signeret med projektets nøgle (`SHA256SUMS.bundle`). Dokumentationen viser, hvordan man kontrollerer den, et image og chartet med `cosign verify` og den offentlige nøgle. |
| GO-134 | SKAL | Hver udgivelses side oplyser, hvor længe versionen får sikkerhedsopdateringer, og hvor sårbarheder rapporteres (`/security/`). |
| GO-135 | SKAL | `hack/release.sh` bygger, skubber og signerer en udgivelse, og `hack/verify-release.sh` kontrollerer den med den offentlige nøgle. cosign, syft og govulncheck kører bag en proxy, der ikke findes, med kun registryet lukket igennem, så et kald til en tjeneste uden for projektets infrastruktur får udgivelsen til at fejle i stedet for at ske i det stille (GO-130). |


## 15. Rækkefølge for implementeringen

1. `api/v1alpha1` og genererede CRD'er. *Færdig*: 22 CRD-tests mod
   API-serveren i testklyngen (`make test-crd`).
2. `internal/chunk` og `internal/store` med unit-tests og Garage.
   *Færdig*: unit- og fuzz-tests, lagertests mod Garage (`make test-s3`).
3. `sdk` med lokal testmode og en filter-komponent. *Færdig*: fil-API'et
   (GO-101 til GO-112), tællere og fejllog (10.1), og filter i CSV.
   Agent-mode kommer med agenten i trin 4.
4. Agenten med `builtin`-signaler, kørt lokalt mod Garage. *Færdig*:
   `internal/agent`, `internal/signal/builtin`, `cmd/alpheus-agent` og
   agent-mode i SDK'et. Agenter og komponenter kører i samme proces mod
   hukommelseslager og mod Garage (`make test-agent-s3`) med
   testcase03's form, genstart af `filter` og af det tilstandsfulde
   `sum`, kilde-genstart (GO-92), fencing (GO-68) og backpressure
   (GO-35). Fejllog og tællere skrives under `_errors/<trin>/<replika>/`
   efter de udgående manifester, så de aldrig står to gange, men kan
   mangle efter et nedbrud lige derimellem (se GO-115 i 16). mTLS til
   hub'en (GO-41) kom med controlleren i trin 5.
5. Controlleren med envtest. *Færdig*: `internal/controller` og
   `cmd/alpheus-controller`. Tests mod en rigtig API-server (`make test`
   henter envtest-binærerne): pod-spec med agenten som native sidecar,
   genstart og `maxRestarts`, `failurePolicy: continue` med lukkede
   strømme og `failedChunks`, ugyldig graf, image uden digest, cancel,
   finalizer, status fra lageret og mTLS til hub'en. CEL-reglerne for
   `resultsTTL` og `ttlAfterFinished` sammenligner nu som varigheder,
   fordi Go-klienten skriver `168h` tilbage som `168h0m0s`. Ikke med
   endnu: ServiceAccount og NetworkPolicy pr. run (6.1 trin 2),
   webhook (GO-23), og at hub'en afviser ældre forsøg (GO-41), da
   certifikaterne er pr. run og ikke pr. forsøg.
6. E2E på Talos i Vagrant-VM'en med testcase01. *Færdig*: `make e2e`
   (`hack/e2e.sh`) bygger agent, controller og testkomponenterne som
   statiske binærer i images på `scratch` (uid 65532, reproducerbare
   digests), skubber dem til Zot, installerer CRD og controller med
   `config/default` og kører `test/e2e`. Den første testcase blev siden
   erstattet af testcase01 i 12.1, som også måler systemet. På testklyngen
   (én node, varmt image) giver standardstørrelsen ca. 1 s opstart, 40
   MB/s gennem to hop og en latens pr. chunk på 300–500 ms. Signalerne
   går gennem hub'en: hvert trin slutter inden for ca. 50 ms efter det
   forrige. Fundet undervejs: hub'en
   fejlede ved første start, fordi signal-CA'en blev oprettet, men
   fejlen fra opslaget blev returneret; controlleren skrev spec'en
   tilbage, når den satte sin finalizer (`168h` blev til `168h0m0s`), og
   bruger nu en patch; og `status.results` manglede regionen, som
   Garage kræver (GO-79).
7. Fan-out og fan-in med testcase02, replikaer med testcase03.
   *Færdig*: begge kører på testklyngen (12.1), og agent-testen
   `TestFanOutFanIn` kører testcase02's form i hukommelsen med
   `make test`. Med standardstørrelsen: testcase02 ca. 23–34 MB/s ind i
   `sink` og en latens pr. chunk på 450–1100 ms (p50–p95), da hver chunk
   læses af to trin; testcase03 ca. 16 MB/s og 570–1000 ms med ti
   `filter`-replikaer på én node. testcase03 blev bygget med
   `bench`-komponenten i stedet for den oprindelige salgsrapport i CSV,
   så testene stadig ikke afhænger af et dataformat.
8. Genstart, backpressure og kaos-test. *Færdig*: testcase04,
   testcase05 og testcase06 kører på testklyngen (12.1), og
   `TestCrashAtEveryWrite` kører med `make test`. Fundet undervejs: en
   genstartet kilde talte de chunks, agenten kasserer (GO-92), med
   igen, og et tilstandsfuldt trin talte alt sit input igen efter en
   genstart; dets fejllog ville også have stået to gange. Et
   tilstandsfuldt trin holder nu tællere og fejllog tilbage til
   `OnEOS`-outputtet er committet, og en kasseret chunk fra en kilde
   committer intet (GO-95, GO-115, GO-118). Efter en gennemgang blev
   tællere og fejllog committet i samme skrivning som `consumed`
   (`observed`), `failurePolicy: continue` lukker den fejlede replikas
   ack-filer, så runnet ikke går i stå, certifikaterne til hub'en blev
   pr. forsøg, og fejlloggen flyttede til `_errors/` (GO-41, GO-97,
   GO-115, GO-118). Testene dræber nu pods med en grace period på ét
   sekund i stedet for tvungen sletning (GO-67). Fundet undervejs: et
   trin med flere udgående manifester talte en chunk igen, hvis det
   stoppede mellem to af dem, så en rapport, et udgående manifest
   allerede har committet, lægges ikke til igen; og hub'en afviste alle
   agenters signaler, fordi den forventede runnets navn uden UID, så
   agenterne kun pollede. På en ny testklynge giver testcase01 stadig
   ca. 41 MB/s og en latens på ca. 250 ms (p50). Runs, der ikke blev
   ryddet op, gjorde Garage langsommere i løbet af dagen: namespacet
   hang, fordi storage-Secret'en blev slettet før runnet, så
   oprydningen manglede nøglerne. Controlleren holder nu Secret'en med
   en finalizer, til data er slettet (GO-20), og e2e-testene fejler,
   hvis et namespace ikke forsvinder inden for 3 minutter. Controlleren
   slettede også et stoppet runs pods, før dets status var skrevet; gik
   skrivningen i konflikt, så det sidste forsøg ud til at være slettet i
   stedet for fejlet. Pods slettes nu først, når fasen er skrevet, og en
   pod, der mangler i cachen, bekræftes slettet hos API-serveren, før den
   tæller som et fejlet forsøg (GO-67).
9. CLI, Helm-chart og første udgivelse `v0.1.0`. Allerede bygget her:
   egen bucket til resultater (GO-129) med en nøgle, der kun kan læse
   dem; testcase03 bruger den. Helm-chartet (`charts/alpheus`) er
   bygget: CRD'en og ClusterRole'n er kopier af de genererede filer
   (`make generate`, kontrolleret af `make check-generated`), CRD'en
   opgraderes med chartet og bliver stående, når det afinstalleres, og
   agentens image skal være pinned ved digest, medmindre `allowTags` er
   sat. `make test-chart` tjekker det renderede chart uden klynge, og
   `make e2e` installerer nu controlleren med chartet i stedet for
   kustomize, med `registry` sat til Zot; kustomize-manifesterne
   (`config/manager`, `config/default`) er fjernet. Chartet er skubbet
   til Zot som OCI-artefakt og hentet derfra igen. CLI'en (9) er bygget
   med standardbibliotekets `flag`, testet mod envtest og prøvet mod
   testklyngen: `run`, `watch`, `get`, `logs`, `chunks`, `cancel`,
   `validate`, `results gc --dry-run` og `version`. Komponenter i andre
   sprog end Go bruger gRPC-kontrakten (`proto/component.proto`) og den
   dokumenterede lokale mode; der er intet Python-SDK i `v0.1.0`.
   Python-referencen er blevet en skabelon: `templates/component-python`
   har et lille modul (`alpheus/`, ca. 330 linjer) med kontrakten og
   lokal mode, uden løfte om et vedligeholdt SDK, og `templates/component-go`
   er det samme i Go. Begge er komponenten `grep`. Komponent-guiden på
   hjemmesiden (`/docs/components/`) gennemgår et run trin for trin med
   eksemplet `examples/log-report` (kilde, `grep` med to replikaer og et
   tilstandsfuldt trin) i begge sprog. Go-skabelonens test kører med
   `make test`, og `make test-templates` kører Python-skabelonen og
   eksemplet i lokal mode i containere og tjekker, at Go og Python giver
   samme resultat. På testklyngen er begge skabeloner kørt i agent-mode
   mellem `bench`-kilde og -sink, og eksemplet er kørt i begge sprog med
   byte-identisk resultat. testcase07 (12.1) kører begge skabeloner i
   agent-mode på testklyngen og slår en pod i hver ihjel midt i strømmen.
   Hjemmesiden har fået `/docs/` med Kom i gang (installation fra kilden
   på testklyngen og eksemplet, bygget med `examples/log-report/images.sh`),
   komponent-guiden og en CLI-reference, der genereres fra CLI'ens egen
   hjælp (`make generate`, WWW-4). Fundet undervejs: lokal mode læste en
   udgangsmappes `manifest.json` som en chunk, så et trins output ikke
   kunne bruges som næste trins input; manifest-filerne springes nu over
   (GO-87). Udgivelsesscriptet (GO-135) er bygget og prøvet mod Zot i
   testklyngen med en testnøgle: CLI'en til Linux og macOS, images til
   amd64 og arm64, chartet, SBOM'er med syft og signaturer med cosign 3.
   To byggerier af samme commit gav samme filer og digests (GO-4), en
   anden nøgle og en ændret fil blev afvist, og testcase01 til 03 bestod
   med det udgivne chart og dets images (`CHART` i `hack/e2e.sh`). Tilbage
   er projektets nøgle, Forgejo, runnerne og registryet.

En STOMP-driver til en ekstern broker (fx Artemis) var planlagt som
trin 9 med GO-39 og GO-42, men er taget ud af planen: signaler gør
kun tingene hurtigere, og `builtin` kræver ingen broker. `driver` i
CRD'en har derfor kun værdien `builtin`.


## 16. Åbne spørgsmål

- Hvor kører Forgejo, runnerne og Zot på `registry.alpheus-dev.org`, og
  hvem driver dem? Se også hjemmesidens `SPEC.md` afsnit 11.
- Hvor længe får en version sikkerhedsopdateringer (GO-134)?

Afgjort 2026-11-01: udgivelser kun på egen infrastruktur i EU (14): Zot
på `registry.alpheus-dev.org`, cosign med projektets egen nøgle og
Forgejo Actions på egne runners. Det afløser beslutningen fra 2026-10-30
om GitHub Container Registry. syft laver SBOM'erne (GO-132).

Afgjort 2026-10-30: Python via gRPC-kontrakten og en referencekomponent
(15, trin 9), manifest-segmenter for hver 1.000 chunks (GO-32), egen
bucket til resultater (GO-129), tællere i commit'et (GO-115), lukning af
fejlede replikaers ack-filer (GO-97), en finalizer på storage-Secret'en
(GO-20) og fejlloggen under `_errors/` (GO-118).
