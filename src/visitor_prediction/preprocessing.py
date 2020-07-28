import pandas as pd


def preprocess_entries(entries, exhibitions, facts):

    exhibitions = _complement_exhibitions(exhibitions, facts)
    entries = _add_is_closed(entries, exhibitions)
    entries = _add_limited_entries(entries, exhibitions)
    entries = _add_exhibition_progress(
        entries,
        exhibitions[exhibitions['special'] == ''])
    entries = _add_exhibition_popularity(
        entries,
        exhibitions[exhibitions['special'] == ''])
    entries = _add_weekdays(entries)
    return entries


def _complement_exhibitions(exhibitions, facts):
    # facts_atttribute should be list of timespans
    for (special, facts_attribute) in [
        ('closing day', 'missing_closed_timespans'),
        ('limited entries', 'limited_entry_timespans')
    ]:
        for closed_timespan in facts[facts_attribute]:
            exhibitions = exhibitions.append(
                pd.DataFrame(
                    columns=exhibitions.columns,
                    data=[{
                        "special": special,
                        'start_date': closed_timespan['start'],
                        'end_date': closed_timespan['end']
                    }]),
                ignore_index=True)
    return exhibitions


def _add_is_closed(entries, exhibitions):
    entries['is_closed'] = 0
    for exhibition in exhibitions[
            exhibitions['special'] == "closing day"].itertuples():
        start = exhibition.start_date
        end = exhibition.end_date
        entries.loc[
            entries['entries'].index.to_series().between(start, end),
            'is_closed'] = 1
    return entries


def _add_limited_entries(entries, exhibitions):
    entries['limited_entries'] = 0
    for exhibition in exhibitions[
            exhibitions['special'] == "limited entries"].itertuples():
        start = exhibition.start_date
        end = exhibition.end_date
        entries.loc[
            entries['entries'].index.to_series().between(start, end),
            'limited_entries'] = 1
    return entries


def _add_exhibition_progress(entries, exhibitions):
    def calc_exhib_progress(date):
        for exhibition in exhibitions.itertuples():
            start = exhibition.start_date
            end = exhibition.end_date
            if start <= date <= end:
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
