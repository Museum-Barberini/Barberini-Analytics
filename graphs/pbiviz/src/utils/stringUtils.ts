interface String {
    trimEx(pattern: RegExp): string;
}

String.prototype.trimEx = function(pattern) {
    return this.replace(RegExp(`^${pattern.source}|${pattern.source}$`, pattern.flags), '');
}
