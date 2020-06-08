# Fuzzy matching insights
## levenshtein
- `Stilleben`: bis 2 i. O., 4 ist `Stadtleben`
- `Picasso`: bis 2 i.O., 3 ist `hasao`
- `Barberini`: bis 2 i.O., 3 ist `barbarei`
- `gogh`: bis 1i.O., 2 ist auch `good` und `noch`, aber auch `van gogh's`
- `architektur`: mit 4 i.O., findet auch englische und spanische varianten
- `fassade`: bis 2 ok, findet auch `façade`
- `etage` - `tagen` ist auch noch so ein Problem
==> Bis 2 geht fast immer

## lower levenshtein/len (`diff/len <= 2/9`)?
- `#claudemonet` bis 0.34 okay
- `Stilleben`: bis 0.24 okay
- `picasso`: bis 0.29 okay
- `Barberini`: bis 0.3 ok
- `van Gogh`: bis 0.25 ok
- `gogh`: bis 0.49 ok
- `Architektur`: bis 0.26 ok
- `monet`: bis 0.19 ok
- `app`: bis 0.3 ok

## pg_trgm
- `fassade`: `façade` liegt bei 0.36
- `Stilleben`: 0.55, dann `stille`
- `Picasso`: bis 0.65 echt, dann valide compounds bis 0.35
  * Idee: längere wörter eher tolerieren?
  * Oder jedes Wort nur dem höchsten Treffer zuordnen
- `barberini`: bis 0.8, dann valide compounds, urls und mehr spellings bis 0.31, dann irgendeine `barbara`
- `gogh`: bis 0.375, dann `go`
- `architektur`: bis 0.51, dann valide compounds/synonyme bis 0.4, dann `archiv`
- problem: `#barockbarberini` wird auf `barberini` gematcht (0.67)
  * Lösung: `barberini` separat matchen und immer nur höchsten Treffer wählen
==> bis 0.65 geht immer

**NEXT:** Write a task that attributes these results to each exhibition/post pair

## Some queries

- SELECT text, word, SIMILARITY(word, 'Architektur') s from post NATURAL join absa.post_word order by s desc limit 1000;
- with (select text, SIMILARITY(word, 'Architektur') s from post NATURAL join absa.post_word)
  SELECT text, word,  where s >= 0.65 order by s desc limit 1000;
- select text from absa.post_word natural join post, SIMILARITY(word, 'barberini') s
  where s >= 0.65;
- select * from absa.target_aspect natural join absa.target_aspect_word;
- SELECT text, w1.word, levenshtein(w1.word, 'gogh') s from post NATURAL join absa.post_word w1 join absa.post_word w0 on w0.post_id = w1.post_id where w1.word_index - 1 = w0.word_index and w0.word like 'van' order by s asc limit 1000;
- 