
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
    const x = Math.round((Number(lon) + 180) * 65535 / 360);
    const y = Math.round((Number(lat) + 90) * 65535 / 180);
    
    let tile = 0;
    
    for (let i = 15; i >= 0; i--) {
        tile = (tile << 1) | ((x >> i) & 1);
        tile = (tile << 1) | ((y >> i) & 1);
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
