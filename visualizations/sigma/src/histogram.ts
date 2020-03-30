export function histogram<T>(map: T[]) {
    let histogram = new Map<T, number>();
    map.forEach(value => {
        histogram.update(value, 0, count => count + 1);
    });
    histogram.forEach((count, key) =>
        histogram.set(key, count / map.length));
    return histogram
}
