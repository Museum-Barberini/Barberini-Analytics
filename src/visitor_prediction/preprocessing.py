import pandas as pd


def _add_is_closed(entries, exhibitions):
    entries['is_closed'] = 0
    for exhibition in exhibitions[
            exhibitions['title'] == "Schließtag / Closing Day"].itertuples():
        start = exhibition.start_date
        end = exhibition.end_date
        entries.loc[
            entries['entries'].index.to_series().between(start, end),
            'is_closed'] = 1
    return entries


def _add_exhibition_progress(entries, exhibitions):
    def calc_exhib_progress(date):
        for exhibition in exhibitions.itertuples():
            start = exhibition.start_date
            end = exhibition.end_date
            if start <= date and date <= end:
                return (date - start).days / (end - start).days
        return -1
    entries['exhibition_progress'] = \
        entries['entries'].index.to_series().apply(calc_exhib_progress)
    return entries


def _add_exhibition_popularity(entries, exhibitions):
    entries['exhibition_popularity'] = 0

    for exhibition in exhibitions.itertuples():
        start = exhibition.start_date
        end = exhibition.end_date
        entries.loc[
            entries['entries'].index.to_series().between(start, end),
            'exhibition_popularity'] = exhibition.popularity
    return entries


def _add_weekdays(entries):
    weekdays_series = entries['entries'].index.to_series().apply(
        lambda date: date.weekday())
    weekdays = pd.get_dummies(weekdays_series, prefix='weekday')
    for col in weekdays.columns:
        entries[col] = weekdays[col]
    return entries


def preprocess_entries(entries, exhibitions):

    entries = _add_is_closed(entries, exhibitions)
    entries = _add_exhibition_progress(
        entries,
        exhibitions[exhibitions['special']])
    entries = _add_exhibition_popularity(
        entries,
        exhibitions[exhibitions['special']])
    entries = _add_weekdays(entries)
    return entries
