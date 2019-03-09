
const PRECISION = 7;
const MULTIPLICATION_FACTOR = calculateMultiplicationFactor();

function calculateMultiplicationFactor() {
    let result = 1;
    
    for (let i = 0; i < PRECISION; i++) {
        result *= 10;
    }
    
    return result;
}

function _calculateTile(lat, lon) {
    const x = BigInt(Math.round((Number(lon) + 180) * 65535 / 360));
    const y = BigInt(Math.round((Number(lat) + 90) * 65535 / 180));

    let tile = 0n;
    
    for (let i = 15n; i >= 0n; i--) {
        tile = (tile << 1n) | ((x >> i) & 1n);
        tile = (tile << 1n) | ((y >> i) & 1n);
    }
    
    return tile;
}

function _convertToFixed(coordinate) {
    return Math.round(Number(coordinate) * MULTIPLICATION_FACTOR);
}

module.exports = {
    calculateTile: _calculateTile,
    convertToFixed: _convertToFixed
}
