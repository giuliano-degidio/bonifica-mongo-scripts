// Lanciare da mongosh
/* SVILUPPO
mongosh "mongodb://giuldegi:bitbros@192.168.248.135:27017/romagna?authSource=admin" ^
  --quiet ^
  --file "C:\Appo09\Mongo_Prod_EXP\046_03_esportazione_su_file_prod_match_diagnosticreport_by_keys_replace.js"
*/
/* PRODUZIONE/TEST
mongosh "mongodb://root:password@localhost:47017/hc40-index?authSource=admin&directConnection=true&readPreference=primaryPreferred" ^
  --quiet ^
  --file "C:\Appo10\Mongo_Prod_EXP\046_03_esportazione_su_file_prod_match_diagnosticreport_by_keys_replace.js"
*/

// COMANDO DI ESECUZIONE DA MONGOSH TEST UBUNTU
/*

/data/mongosh/bin/mongosh "mongodb://root:password@mongo-rs-1.mongo-rs-svc.mongodb.svc.cluster.local:27017/hc40-index-bonifica?authSource=admin&directConnection=true&readPreference=primary" \
  --quiet \
  --file "/data/Mongo_Sh_Script/046_03_esportazione_su_file_prod_match_diagnosticreport_by_keys_replace.js"

  spostare il file da windows a ubuntu kubernate:
 Get-Content -Raw "C:\Users\giuldegi\OneDrive - Engineering Ingegneria Informatica S.p.A\Desktop\ENG\SANITA\Romagna\CDR-Mongo\Svil\BONIFICA_CDR\046_03_esportazione_su_file_prod_match_diagnosticreport_by_keys_replace.js" |
kubectl -n ellipse-index exec -i ubuntu-mongosync-6845564564-zvnn9 -- sh -c 'cat > /data/Mongo_Sh_Script/046_03_esportazione_su_file_prod_match_diagnosticreport_by_keys_replace.js'

  spostare il file diagnosticreport_impacted_all.tar da windows a ubuntu kubernate:
 Get-Content -Raw "C:\Appo10\Mongo_Prod_EXP\EXP\diagnosticreport_impacted_all.tar" |
kubectl -n ellipse-index exec -i ubuntu-mongosync-6845564564-zvnn9 -- sh -c 'cat > /data/Mongo_Sh_Script/EXP/diagnosticreport_impacted_all.tar'
*/

// OUTPUT DETTAGLI COMPLETATI.
...
