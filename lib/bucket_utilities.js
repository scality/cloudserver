module.exports = {

	markerFilter: function(marker, array){
		for(var i=0; i<array.length; i++){
			if(marker > array[i]){
				array.shift();
				i--;
			} else {
				break;
			}
		}
		return array;
	},

	prefixFilter: function(prefix, array){
		for(var j=0; j<array.length; j++){
			if(array[j].indexOf(prefix) !== 0){
				array.splice(j, 1);
			}
		}
			return array;
	},

};