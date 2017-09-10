function calculate(formula) {
    var atom=[];
    atom["H"]= 1;
    atom["He"]= 4;
    atom["C"]= 12;
    atom["N"]= 14;
    atom["O"]= 16;
    atom["F"]= 19;
    var uppercase="ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    var lowercase="abcdefghijklmnopqrstuvwxyz";
    var number="0123456789";

    total=[]; level=0; total[0]=0; i=0; mass=0; name=''; percision=8;
    elmass=[];
    for (i=0; i<elmass.length;i++) {
        elmass[i]=null;
    }
    elmass[0]=[];
    for (i=0; i<elmass[0].length;i++) {
        elmass[0][i]=0;
    }
    i=0;
    while (formula.charAt(i)!="") {
        if ((uppercase+lowercase+number+"()").indexOf(formula.charAt(i))==-1)
            i++;
        while (formula.charAt(i)=="(") {
            level++;
            i++;
            total[level]=0;
            elmass[level]=new Array();
            for (h=0; i<elmass[level].length;h++) {
                elmass[level][i]=0;
            }
        }
        if (formula.charAt(i)==")") {
            mass=total[level];
            name='';
            level--;
        }
        else if (uppercase.indexOf(formula.charAt(i))!=-1) {
            name=formula.charAt(i);
            while (lowercase.indexOf(formula.charAt(i+1))!=-1 && formula.charAt(i+1)!="")
                name+=formula.charAt(++i);
            mass=atom[name];
            massStr=mass+"";
            if (massStr.indexOf(".")!=-1)
                masspercis=(massStr.substring(massStr.indexOf(".")+1,massStr.length)).length;
            else
                masspercis=0;
            percision=(percision==8 || percision>masspercis)?masspercis:percision;
        }
        var amount="";
        while (number.indexOf(formula.charAt(i+1))!=-1 && formula.charAt(i+1)!="")
            amount+=formula.charAt(++i);
        if (amount=="") amount="1";
        total[level]+=mass*parseInt(amount);
        if (name=="") {
            for (ele in elmass[level+1]) {
                totalnumber=parseInt(elmass[level+1][ele])*amount
                if (elmass[level][ele]==null)
                    elmass[level][ele]=totalnumber;
                else
                    elmass[level][ele]=totalnumber+parseInt(elmass[level][ele]);
            }
        }
        else {
            if (elmass[level][name]==null)
                elmass[level][name]=amount;
            else
                elmass[level][name]=parseInt(elmass[level][name])+parseInt(amount);
        }
        i++;
    }
    return total[0];
}


function insertMolecules() {
    insertMoleculeInfos();
    db.molecules.update({name:"Water"}, {"$set":{formula:"H2O"}});
    db.molecules.update({formula:/H/}, {$push:{atoms:"H"}});
    db.molecules.update({formula:/C/}, {$push:{atoms:"C"}}, {multi:true});
    db.molecules.update({formula:/O/}, {$push:{atoms:"O"}}, {multi:true});

    db.molecules.find().forEach(
        function(molecule){
            molecule.mass = calculate(molecule.formula);
            db.molecules.save(molecule);
        }
    );
}

function insertMoleculeInfos() {
    db.molecules.insert({name:"Water", formula:"H2O2"});
    db.molecules.insert({name:"Hydrogen Peroxide", formula:"H2O2"});
    db.molecules.insert({name:"Carbon Monoxide", formula:"CO"});
    db.molecules.insert({name:"Carbon Dioxide", formula:"CO2"});
}

function aggregate() {
    db.molecules.aggregate([{"$unwind" :"$atoms"}, {"$group":{_id:"$atoms", avgMass: {$avg:"$mass"}}}])
}


