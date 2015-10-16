# TODO

## Sovrascrittura di dati archiviati

1.  Prendo la lista dei dataset coinvolti e l'intervallo di tempo dei dati
    nuovi.
2.  Lancio un repack su questi dataset (così i dati archived sono sicuramente in
    last).
3.  Ricreo i dataset coinvolti vuoti.
4.  Per ogni file in .archive, controllo se è coinvolto nella sovrascrittura.
    Se lo è, lo importo nei dataset clonati e tengo traccia del nome del file.
5.  Importo i dati nuovi.
6.  Lancio un repack sui dataset clonati.
7.  Ogni file originale coinvolto presente in .archive viene eliminato.
8.  Copio i dati negli .archive/last dei dataset clonati nei dataset sorgenti.
9.  Se rimane qualche dato in linea, lo importo normalmente (oppure li sposto da
    qualche parte per un'importazione successiva, ma si deve cambiare l'mtime).
10. Lancio un arki-check sui dataset sorgenti.


## Sovrascrittura delle flag del controllo di qualità

Funziona come prima, con l'unica differenza che

4. Non li importo, ma li carico in un sqlite.
5. Non li importo ma vado a sovrascrivere le flag dei dati vecchi. Poi importo
   i dati risultanti.


## Eliminazione di dati dai dataset

Se i dati sono in linea, basta usare `arki-check --remove=FILE`.

Funziona come per la sovrascrittura, ma:

5. Lancio `arki-check --remove=FILE`
