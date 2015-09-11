module.exports = {

	markerFilter: function(marker, array){
		var length = array.length;
		for(var i=0; i<length; i++){
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
		var length = array.length;
		for(var j=0; j<length; j++){
			if(array[j].indexOf(prefix) !== 0){
				array.splice(j, 1);
			}
		}
			return array;
	},

};