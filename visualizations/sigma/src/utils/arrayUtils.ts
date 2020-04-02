interface Array<T> {
    /** Flattens this array of arrays into a single array. */
    flatten<T extends U[], U>(this: T[]): T;
    
    /** Applies a fold function on each element of this array and flattens the results. */
    fold<U>(this: T[], fun: (value: T) => U[]): U[];
    
    /** C'mon. You know SQL. You surely can figure out what this does. */
    groupBy<K, V>(funKey: (value: T) => K, funValue: (value: T) => V): Map<K, V[]>;
    
    /**
     * Creates a map from this array, using the elements and keys and creating the values from a
     * map function.
     */
    mapEx<U>(funValue: (value: T, index: number) => U): Map<T, U>;
    
    /**
     * Combines this array with a second one of equal length, applying a zip function on each
     * i-th tuple of array entries.
     */
    zip<U, V>(other: U[], fun: (x: T, y: U) => V): V[];
}


Array.prototype.flatten = function<T extends U[], U>(this: T[]) {
    return [].concat.apply([], this);
};

Array.prototype.fold = function<T, U>(this: T[], fun: (value: T) => U[]) {
    return this.map(fun).flatten();
};

Array.prototype.groupBy = function<T, K, V>(
    this: T[], funKey: (value: T) => K,
    funValue: (value: T) => V) {
    const map = new Map<K, V[]>();
    this.forEach(value => {
        let group = map.getOrSetDefault(funKey(value), () => []);
        group.push(funValue(value));
    });
    return map;
};

Array.prototype.mapEx = function<T, U>(this: T[], funValue: (value: T, index: number) => U) {
    return new Map(this.map((value, index) => [value, funValue(value, index)]));
};

Array.prototype.zip = function <T, U, V>(this: T[], other: U[], fun: (x: T, y: U) => V) {
    console.assert(this.length == other.length);
    return this.map((x, i) => fun(x, other[i]));
};
