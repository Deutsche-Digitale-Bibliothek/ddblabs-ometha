echo -e "\n------ Normaler Vorgang, sollte funktionieren\n\n"
ometha default -b https://digital.sulb.uni-saarland.de/viewer/oai/ -m mets -d Test -s hk
sleep 2.5
printf "\033c"
echo -e "\n------ Falsche Angabe, erwarte Fehler von der Schnittstelle\n\n"
ometha default -b https://digital.sulb.uni-saarland.de/viewer/oai/ -m mets -d Test -s dassetdasesnichtgibt
sleep 2.5
printf "\033c"
echo -e "\n------ Defekte URL\n\n"
ometha default -b https://kaputt.sulb.uni-saarland.de/viewer/oai/ -m mets -d Test
sleep 2.5
printf "\033c"
echo -e "\n------ ID File - sollte funktionieren\n\n"
ometha ids -d Test -i tests/test_ids.yaml
sleep 2.5
printf "\033c"
echo -e "\n------ Config Modus, sollte funktionieren\n\n"
ometha conf -c tests/test_config.yaml
sleep 2.5
printf "\033c"
echo -e "\n------ ID File nicht richtig referenziert\n\n"
ometha ids -d Test -i tests/test_ids_gibtsnicht.yaml
sleep 2.5
printf "\033c"
echo -e "\n------ Configfile nicht richtig referenziert\n\n"
ometha conf -c config/gibtsnicht.yaml -a
# sleep 2.5
# printf "\033c"
# echo -e "\n\n\nHarvesting mit ResumptionToken\n\n"
# ometha default -b https://oai.deutsche-digitale-bibliothek.de --resumptiontoken="rows=300@@searchMark=QW9KNG00em1nUGdDUHdGV1drMVNUelpXUTBWQ04xVlpRalJWUWs5RlRUTllTMUZWUTFGSlZUZEdVZz09@@set=10443700598299947xcYN@@from=0001-01-01T00:00:00Z@@total=19999@@until=9999-12-31T23:59:59Z@@metadataPrefix=ddb" -m ddb