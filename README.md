# bonifica-mongo-scripts

Repository di **script mongosh** + **pipeline runner** per bonifica/riconciliazione dati su MongoDB.

## Prerequisiti
- `jq`
- `mongosh` disponibile in: `/data/mongosh/bin/mongosh`
- accesso di rete al MongoDB indicato nella sezione `envs` del file di pipeline
- (opzionale) `timeout` e `zip` per timeout per-step e creazione bundle zip

## Struttura repo
- `pipeline/`  
  Contiene la pipeline JSON (config) con la lista ordinata degli step.
- `scripts/`  
  Contiene gli script `.js` eseguiti da `mongosh`.
- `lib/`  
  Librerie JS condivise (es. lettura contesto).
- `EXP/`  
  Output esportazioni/file generati dagli script (creata automaticamente se non esiste).
- `logs/`  
  Log di esecuzione per step (creata automaticamente se non esiste).
- `state/`  
  Stato della pipeline (per modalità resume) (creata automaticamente se non esiste).

## Esecuzione (modalità repo-clone)

Dalla root del repo:

```bash
chmod +x ./run_pipeline.sh
./run_pipeline.sh --config pipeline/pipeline_all_script.json --env k8s-bonifica --resume
```

Per rilanciare tutto da zero (nuovo `runId`):

```bash
./run_pipeline.sh --config pipeline/pipeline_all_script.json --env k8s-bonifica --rerun-all
```

Esecuzione specificando un env (template):

```bash
./run_pipeline.sh --config pipeline/pipeline_all_script.json --env <NOME_ENV>
```

### Opzioni utili
- `--stop-on-error` / `--continue-on-error`
- `--tee-to-console` / `--no-tee-to-console`

## Dove finiscono i file
Per ogni esecuzione viene creato un `runId` (UTC) e i file vengono salvati in:
- **stato pipeline**: `./state/<pipelineName>/<env>/<runId>/`
- **log**: `./logs/<pipelineName>/<env>/<runId>/`
- **export/output**: `./EXP/`

La pipeline mantiene anche:
- `./state/<pipelineName>/<env>/latest_run.json` (ultimo `runId`, usato da `--resume`)

A fine run il runner crea anche:
- `./state/<pipelineName>/<env>/<runId>/results_summary.jsonl`  
  (contiene tutte le righe JSON nei log che hanno `type="result"`)

## Configurazione
Il file principale è:
- `pipeline/pipeline_all_script.json`

### Campi importanti
- `tools.mongoshPath`: path di `mongosh` (default: `/data/mongosh/bin/mongosh`)
- `paths.scriptsDir`: in modalità repo-clone deve puntare a `./scripts`
- `paths.expDir`, `paths.logsDir`, `paths.stateDir`: in modalità repo-clone conviene mantenerli su `./EXP`, `./logs`, `./state`
- `envs.<nome>.mongo.uriBase`, `dbName`, `uriParams`: configurazione Mongo per ambiente

Esempio esecuzione con env:

```bash
./run_pipeline.sh --config pipeline/pipeline_all_script.json --env k8s-bonifica --resume
```

## Resume e lock
- In modalità `--resume`, gli step già `SUCCESS` vengono saltati.
- In modalità `--rerun-all`, viene creato un nuovo `runId` e la pipeline riparte da zero.
- Se `settings.locking=true` nel JSON, il runner crea un lock file:
  - `./state/<pipelineName>/<env>/run.lock`

Se trovi un lock file e sei sicuro che non ci siano esecuzioni in corso, puoi rimuoverlo manualmente:

```bash
rm -f ./state/<pipelineName>/<env>/run.lock
```

## Troubleshooting

### 1) `Missing command: jq`
Installa `jq` (e assicurati che sia nel `PATH`):
- Debian/Ubuntu: `sudo apt-get install -y jq`
- RHEL/CentOS: `sudo yum install -y jq` (o `dnf`)
- macOS: `brew install jq`

### 2) `Config file not found: ...`
Stai passando un path errato al `--config`. Esempio corretto dalla root:

```bash
./run_pipeline.sh --config pipeline/pipeline_all_script.json --env k8s-bonifica --resume
```

### 3) `env 'XXX' not found or envs[XXX].mongo.uriBase missing`
Nel JSON non esiste `envs.XXX` oppure mancano i campi mongo. Controlla:
- `envs.<nome>.mongo.uriBase`
- `envs.<nome>.mongo.dbName`

### 4) `STEP_ERROR: script not found: .../qualcosa.js`
Il runner costruisce lo script path come:
- `scriptsDir + "/" + stepFile`

Quindi devi verificare:
- `paths.scriptsDir` nel JSON (in repo-clone: `./scripts`)
- che il file indicato in `steps[].file` esista davvero in `scripts/`

### 5) `LOCK EXISTS: .../run.lock`
C’è un’esecuzione in corso (o un lock rimasto). Se sei sicuro che non ci siano run attivi, rimuovi il lock:

```bash
rm -f ./state/<pipelineName>/<env>/run.lock
```

### 6) Errori di connessione Mongo / autenticazione
Il runner costruisce la URI come:
- `uriBase + "/" + dbName + "?" + uriParams`

Controlla nel JSON:
- `envs.<nome>.mongo.uriBase` (host/porta/credenziali)
- `envs.<nome>.mongo.dbName`
- `envs.<nome>.mongo.uriParams` (es. `authSource=admin`, `directConnection=true`)

Per diagnosi rapida, prova a connetterti manualmente:

```bash
/data/mongosh/bin/mongosh "<URI_COMPLETA>"
```

### 7) Timeout non applicato
Se nel config imposti `timeoutSec > 0` ma il sistema non ha il comando `timeout`, il runner lo segnala nel log e continuerà senza timeout. Installa `coreutils`/`timeout` oppure esegui su una macchina che lo include.

### 8) ZIP non creato
Se manca il comando `zip`, il runner salta la creazione del bundle ZIP e lo segnala nei log.

## Comandi utili

### Vedere l’ultimo runId usato dal resume
```bash
cat ./state/pipeline_all_script/k8s-bonifica/latest_run.json
```

### Cercare l’errore in master log
```bash
# sostituisci <runId>
grep -n "STEP_FAILED\|STEP_ERROR\|STOP_ON_ERROR" ./logs/pipeline_all_script/k8s-bonifica/<runId>/master.log
```
### Copiare la cartella `Mongo_Sh_Script\lib` da Windows al pod Ubuntu su Kubernetes (`kubectl cp`)

Esempio da **PowerShell** per copiare `.\lib` dentro `/data/Mongo_Sh_Script` nel pod.

```powershell
# ---- Parametri (personalizza questi) ----
$Kubectl     = "C:\kubectl\kubectl.exe"
$Kubeconfig  = "C:\kubeconfig\config-test.yaml"
$KubeContext = "kubernetes-admin@K8DOSSIERTEST"

$Namespace = "ellipse-index"

# Opzione A (consigliata): seleziona il pod automaticamente tramite label
# Imposta la label corretta del tuo deployment (esempio: app=ubuntu-mongosync)
$PodSelector = "app=ubuntu-mongosync"

# Opzione B: specifica direttamente il nome del pod (se preferisci)
# $PodName = "ubuntu-mongosync-6845564564-zvnn9"

$RemoteBaseDir = "/data/Mongo_Sh_Script"

# Directory locale dove hai clonato/coppiato Mongo_Sh_Script
$LocalProjectDir = "C:\Appo10\EXP_POD\Mongo_Sh_Script"
$LocalLibDir     = Join-Path $LocalProjectDir "lib"

# ---- Seleziona il contesto ----
& $Kubectl --kubeconfig $Kubeconfig config use-context $KubeContext

# ---- Risolvi il pod (scegli A o B) ----
if ($PodSelector -and $PodSelector.Trim().Length -gt 0) {
  $PodName = (& $Kubectl --kubeconfig $Kubeconfig -n $Namespace get pod -l $PodSelector -o jsonpath="{.items[0].metadata.name}")
}

if (-not $PodName) {
  throw "Pod non risolto. Imposta `$PodSelector (label) oppure `$PodName (nome pod)."
}

# ---- Copia `lib` nel pod ----
Set-Location $LocalProjectDir
& $Kubectl --kubeconfig $Kubeconfig cp $LocalLibDir "${Namespace}/${PodName}:${RemoteBaseDir}"
```

Note:
- `kubectl cp` copierà la cartella `lib` in: `${RemoteBaseDir}/lib`
- Se hai più pod che matchano la label, viene scelto il primo (`.items[0]`).
