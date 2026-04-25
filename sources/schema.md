# Źródło danych: DDoS Network Traffic Dataset

## Pochodzenie
- **Źródło:** Kaggle - devendra416/ddos-datasets
- **Format:** CSV, separator `,`, nagłówek w pierwszym wierszu
- **Rozmiar:** ~170 MB, ~225 tys. wierszy
- **Klasy:** `ddos` (atak) / `Benign` (ruch normalny)

## Kluczowe kolumny

| Kolumna          | Typ     | Opis                            |
|------------------|---------|---------------------------------|
| Label            | string  | Klasa przepływu: ddos / Benign  |
| Src IP           | string  | Adres IP źródłowy               |
| Dst Port         | int     | Port docelowy                   |
| Flow Duration    | long    | Czas trwania przepływu (µs)     |
| Tot Fwd Pkts     | int     | Łączna liczba pakietów forward  |
| Tot Bwd Pkts     | int     | Łączna liczba pakietów backward |
| Flow ID          | string  | Usuwana - unikalne ID przepływu |
| Unnamed: 0       | int     | Usuwana - artefakt eksportu CSV |

## Warstwy przetwarzania

| Warstwa | Opis                                                  |
|---------|-------------------------------------------------------|
| Bronze  | CSV > JSON, usunięcie zbędnych kolumn                 |
| Silver  | Zamiana ±inf na null, imputacja średnią, deduplikacja |
| Gold    | 4 tabele analityczne (top IPs, porty, statystyki)     |

## Znane problemy z danymi
- Kolumny float/double mogą zawierać wartości `Infinity` - obsługiwane w Silver
- `Unnamed: 0` to artefakt `df.to_csv()` bez `index=False` - usuwana w Bronze
