interface String {
    trimEx(pattern: RegExp): string;
}


/** Removes all leading and trailing occurences of @param pattern from this string. */
String.prototype.trimEx = function(pattern) {
    return this.replace(RegExp(`^${pattern.source}|${pattern.source}$`, pattern.flags), '');
};
