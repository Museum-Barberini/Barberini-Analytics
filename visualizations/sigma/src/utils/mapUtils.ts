interface Map<K, V> {
	/**
	 * Returns the value associated with a key in this map. If no value is associated, add an
	 * association with a default value. This is a direct equivalent of good old Smalltalk's
	 * #at:ifAbsentPut:.
	 * @param funDefault The default value. If it is a parameterless function, it will be called,
	 * otherwise, it will be stored directly.
	 */
	getOrSetDefault(key: K, funDefault: (V | (() => V)));
	
	/**
	 * Applies a map function on each value of this map and returns the results in a new map,
	 * using this map's keys again.
	 */
	mapEx<U>(fun: (value: V) => U): Map<K, U>;
	
	/**
	 * Updates the value of a key in this map by applying a patch function on it. If the key is
	 * not present, set it to a default value before, which follows the semantics from
	 * getOrSetDefault().
	 */
	update(key: K, funDefault: (V | (() => V)), funPatch: (old: V) => V): Map<K, V>;
}


Map.prototype.getOrSetDefault = function<K, V>(
    this: Map<K, V>, key: K, defaultValue: (V | (() => V)))
{
    if (this.has(key))
        return this.get(key);
    const value = typeof defaultValue === "function"
        ? defaultValue.call(defaultValue)
        : defaultValue;
    this.set(key, value);
    return value;
};

Map.prototype.mapEx = function<K, V, U>(this: Map<K, V>, fun: (value: V) => U) {
    return new Map(Array.from(this, ([key, value]) => [key, fun(value)]));
};

Map.prototype.update = function<K, V>(
    this: Map<K, V>, key: K, defaultValue: (V | (() => V)), funPatch: (old: V) => V)
{
    const old = this.getOrSetDefault(key, defaultValue);
    this.set(key, funPatch(old));
    return this;
};
