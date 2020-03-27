interface Map<K, V> {
    getOrSetDefault(key: K, funDefault: (V | (() => V)));
	mapEx<U>(fun: (value: V) => U): Map<K, U>;
	update(key: K, funDefault: (V | (() => V)), funPatch: (old: V) => V);
}

// Returns the value associated with a key in this map. If no value is associated, add an association via a default value.
// If the default value is a (parameterless) function, call it, otherwise store it directly.
// This is a direct equivalent of good old Smalltalk's #at:ifAbsentPut:.
Map.prototype.getOrSetDefault = function<K, V>(this: Map<K, V>, key: K, defaultValue: (V | (() => V))) {
	if (this.has(key))
		return this.get(key);
	const value = typeof defaultValue === "function"
		? defaultValue.call(defaultValue)
		: defaultValue;
	this.set(key, value);
	return value;
}

// Applies a map function on each value of this map and returns the results in a new map, using this map's keys again.
Map.prototype.mapEx = function<K, V, U>(this: Map<K, V>, fun: (value: V) => U) {
    return new Map(Array.from(this, ([key, value]) => [key, fun(value)]));
}

// Updates the value of a key in this map by applying a patch function on it. If the key is not present, set it
// to a default value before, which follows the semantics from getOrSetDefault().
Map.prototype.update = function<K, V>(this: Map<K, V>, key: K, defaultValue: (V | (() => V)), funPatch: (old: V) => V) {
	const old = this.getOrSetDefault(key, defaultValue);
	this.set(key, funPatch(old));
	return this;
}
