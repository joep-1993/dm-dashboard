var today =  new Date();
var input = "INPUT/OUTPUT";
var replace = "REPLACEMENTS";
var urls_processed = [];
var sheet = SpreadsheetApp.getActive();

//CAT-CAT
var newValues = [];
var oldValues = [];
var newmaincatValues = [];

//FACET-FACET
var oldFValues = [];
var newFValues = [];
var catFurls = [];
var catFurlsNew = [];

//CAT-FACET
var old_cat_to_facet = [];
var newer_cat_to_facet = [];
var new_cat_to_facet = [];

//FACET-CAT
var old_facet_to_cat = [];
var new_facet_to_cat = [];

var results_a  = [];

function finish(){
    sortSheet();
      var ss = sheet.getSheetByName(input);
      var checkStatus = ss.getRange(2,3).getValue();

    getOldandNewValues("CAT-CAT");
    getOldandNewValues("FACET-FACET");
    getOldandNewValues("CAT-FACET");
    getOldandNewValues("FACET-CAT");
    //fixUrlsManually();
    fixUrls();

    if (checkStatus=="JA"){
        checkStatuscodesManually();
    }
}

function main(){
    sortSheet();
      var sheet = SpreadsheetApp.getActive();
      var ss = sheet.getSheetByName(input);
      var lr = ss.getLastRow();

      for (var i=5;i<lr+1;i++){
        var url = ss.getRange(i,1).getValue();
          urls_processed.push(url);
      }

      var checkGA = ss.getRange(1,3).getValue();
      var checkStatus = ss.getRange(2,3).getValue();

//cat-->cat
  getOldandNewValues("CAT-CAT");

  if (oldValues.length>0){
    Logger.log("     checking GA for url's containing ["+oldValues+"]");
        if (checkGA=="JA"){
          for (var i=0;i<oldValues.length;i++){
            getUrls(oldValues[i],"");
          }
        }
  }

//facet-->facet
  getOldandNewValues("FACET-FACET");

  if (oldFValues.length>0){
    Logger.log("     checking GA for url's containing ["+oldFValues+"]");
        if (checkGA=="JA"){
          for (var i=0;i<oldFValues.length;i++){

              if (catFurls[i]!==""){
                  getUrls(oldFValues[i],catFurls[i]);
              }
              else {
                  getUrls(oldFValues[i],"");
              }
          }
        }
  }

//cat-->facet
  getOldandNewValues("CAT-FACET");

  if (old_cat_to_facet.length>0){
    Logger.log("     checking GA for url's containing ["+old_cat_to_facet+"]");
        if (checkGA=="JA"){
          for (var i=0;i<old_cat_to_facet.length;i++){
              getUrls(old_cat_to_facet[i],"");
          }
        }
  }

//facet-->cat
  getOldandNewValues("FACET-CAT");

  if (old_facet_to_cat.length>0){
    Logger.log("     checking GA for url's containing ["+old_facet_to_cat+"]");
        if (checkGA=="JA"){
          for (var i=0;i<old_facet_to_cat.length;i++){
            getUrls(old_facet_to_cat[i],"");
          }
        }
  }
  if (results_a.length>0){
    outputToSpreadsheet_a();

    fixUrls();

      if (checkStatus=="JA"){
        checkStatuscodesManually();
      }
  }
}

function sortSheet(){
      var sheet = SpreadsheetApp.getActive();
      var ss = sheet.getSheetByName(input);
      var lr = ss.getLastRow();

      if (lr>4){
        var lc = ss.getLastColumn();
        var range = ss.getRange(5,1,lr-4,lc);
          range.sort(1);
          range.sort(3);
      }
}

function getUrls(contains_a,filters_a) {

  const propertyId = 260071962;
  const request = AnalyticsData.newRunReportRequest();
    
    request.dimensions = [
        { "name": "pagePath" }
      ];    
    
    request.metrics = [        
         { "name": "sessions" }
      ];
    request.dateRanges = [
        {
           "startDate": "2021-01-01" //
          ,"endDate": "today"   //
        }
      ];
    request.limit = 4000;
    request.metricFilter = {"filter":{"fieldName":"sessions","numericFilter":{"operation":"GREATER_THAN","value":{"doubleValue":"0"}}}};
    request.orderBys = [{"metric":{"metricName":"sessions"},"desc":true}];
    request.keepEmptyRows = false; 

    if (filters_a==""){
        request.dimensionFilter = {
                "andGroup": {
                  "expressions": [
                    {
                        "filter": {
                          "stringFilter": {
                            "value": contains_a,
                            "caseSensitive": false,
                            "matchType": "CONTAINS"
                          },
                          "fieldName": "pagePath"
                        }
                    },
                    {
                      "notExpression": {
                        "filter": {
                          "stringFilter": {
                            "value": "device=",
                            "caseSensitive": false,
                            "matchType": "CONTAINS"
                          },
                          "fieldName": "pagePath"
                        }
                      }
                    },
                    {
                      "notExpression": {
                        "filter": {
                          "stringFilter": {
                            "value": "/sitemap/",
                            "caseSensitive": false,
                            "matchType": "CONTAINS"
                          },
                          "fieldName": "pagePath"
                        }
                      }
                    },
                    {
                      "notExpression": {
                        "filter": {
                          "stringFilter": {
                            "value": "sortby=",
                            "caseSensitive": false,
                            "matchType": "CONTAINS"
                          },
                          "fieldName": "pagePath"
                        }
                      }
                    },
                    {
                      "notExpression": {
                        "filter": {
                          "stringFilter": {
                            "value": "/filters/",
                            "caseSensitive": false,
                            "matchType": "CONTAINS"
                          },
                          "fieldName": "pagePath"
                        }
                      }
                    },
                    {
                      "notExpression": {
                        "filter": {
                          "stringFilter": {
                            "value": "/page_",
                            "caseSensitive": false,
                            "matchType": "CONTAINS"
                          },
                          "fieldName": "pagePath"
                        }
                      }
                    },
                    {
                      "notExpression": {
                        "filter": {
                          "stringFilter": {
                            "value": "/cadeaus_gadgets_culinair/meubilair_",
                            "caseSensitive": false,
                            "matchType": "CONTAINS"
                          },
                          "fieldName": "pagePath"
                        }
                      }
                    },
                    {
                      "notExpression": {
                        "filter": {
                          "stringFilter": {
                            "value": "/l/",
                            "caseSensitive": false,
                            "matchType": "CONTAINS"
                          },
                          "fieldName": "pagePath"
                        }
                      }
                    },                   
                    {
                      "notExpression": {
                        "filter": {
                          "stringFilter": {
                            "value": "shop_id=",
                            "caseSensitive": false,
                            "matchType": "CONTAINS"
                          },
                          "fieldName": "pagePath"
                        }
                      }
                    },
                    {
                      "notExpression": {
                        "filter": {
                          "stringFilter": {
                            "value": "/kantoorartikelen/mode_",
                            "caseSensitive": false,
                            "matchType": "CONTAINS"
                          },
                          "fieldName": "pagePath"
                        }
                      }
                    },
                    {
                      "notExpression": {
                        "filter": {
                          "stringFilter": {
                            "value": "/meubilair/mode_",
                            "caseSensitive": false,
                            "matchType": "CONTAINS"
                          },
                          "fieldName": "pagePath"
                        }
                      }
                    },
                    {
                      "notExpression": {
                        "filter": {
                          "stringFilter": {
                            "value": "/klussen/huis_tuin",
                            "caseSensitive": false,
                            "matchType": "CONTAINS"
                          },
                          "fieldName": "pagePath"
                        }
                      }
                    },   
                    {
                      "notExpression": {
                        "filter": {
                          "stringFilter": {
                            "value": "(other)",
                            "caseSensitive": false,
                            "matchType": "EXACT"
                          },
                          "fieldName": "pagePath"
                        }
                      }
                    }, 
                    {
                      "notExpression": {
                        "filter": {
                          "stringFilter": {
                            "value": "\/c\/.+\\+",
                            "caseSensitive": false,
                            "matchType": "PARTIAL_REGEXP"
                          },
                          "fieldName": "pagePath"
                        }
                      }
                    },                 
                    {
                      "notExpression": {
                        "filter": {
                          "stringFilter": {
                            "value": "(not set)",
                            "caseSensitive": false,
                            "matchType": "EXACT"
                          },
                          "fieldName": "pagePath"
                        }
                      }
                    }                                                                                                                      
                  ]
                }
        };
    }
    else {
      request.dimensionFilter = {
              "andGroup": {
                "expressions": [
                  {
                      "filter": {
                        "stringFilter": {
                          "value": contains_a,
                          "caseSensitive": false,
                          "matchType": "CONTAINS"
                        },
                        "fieldName": "pagePath"
                      }
                  },
                  {
                      "filter": {
                        "stringFilter": {
                          "value": filters_a,
                          "caseSensitive": false,
                          "matchType": "CONTAINS"
                        },
                        "fieldName": "pagePath"
                      }
                  },                  
                  {
                    "notExpression": {
                      "filter": {
                        "stringFilter": {
                          "value": "device=",
                          "caseSensitive": false,
                          "matchType": "CONTAINS"
                        },
                        "fieldName": "pagePath"
                      }
                    }
                  },
                  {
                    "notExpression": {
                      "filter": {
                        "stringFilter": {
                          "value": "/sitemap/",
                          "caseSensitive": false,
                          "matchType": "CONTAINS"
                        },
                        "fieldName": "pagePath"
                      }
                    }
                  },
                  {
                    "notExpression": {
                      "filter": {
                        "stringFilter": {
                          "value": "/l/",
                          "caseSensitive": false,
                          "matchType": "CONTAINS"
                        },
                        "fieldName": "pagePath"
                      }
                    }
                  },
                  {
                    "notExpression": {
                      "filter": {
                        "stringFilter": {
                          "value": "sortby=",
                          "caseSensitive": false,
                          "matchType": "CONTAINS"
                        },
                        "fieldName": "pagePath"
                      }
                    }
                  },
                  {
                    "notExpression": {
                      "filter": {
                        "stringFilter": {
                          "value": "/filters/",
                          "caseSensitive": false,
                          "matchType": "CONTAINS"
                        },
                        "fieldName": "pagePath"
                      }
                    }
                  },
                  {
                    "notExpression": {
                      "filter": {
                        "stringFilter": {
                          "value": "/page_",
                          "caseSensitive": false,
                          "matchType": "CONTAINS"
                        },
                        "fieldName": "pagePath"
                      }
                    }
                  },
                  {
                    "notExpression": {
                      "filter": {
                        "stringFilter": {
                          "value": "/cadeaus_gadgets_culinair/meubilair_",
                          "caseSensitive": false,
                          "matchType": "CONTAINS"
                        },
                        "fieldName": "pagePath"
                      }
                    }
                  },
                  {
                    "notExpression": {
                      "filter": {
                        "stringFilter": {
                          "value": "shop_id=",
                          "caseSensitive": false,
                          "matchType": "CONTAINS"
                        },
                        "fieldName": "pagePath"
                      }
                    }
                  },
                  {
                    "notExpression": {
                      "filter": {
                        "stringFilter": {
                          "value": "/kantoorartikelen/mode_",
                          "caseSensitive": false,
                          "matchType": "CONTAINS"
                        },
                        "fieldName": "pagePath"
                      }
                    }
                  },
                  {
                    "notExpression": {
                      "filter": {
                        "stringFilter": {
                          "value": "/meubilair/mode_",
                          "caseSensitive": false,
                          "matchType": "CONTAINS"
                        },
                        "fieldName": "pagePath"
                      }
                    }
                  },
                  {
                    "notExpression": {
                      "filter": {
                        "stringFilter": {
                          "value": "/klussen/huis_tuin",
                          "caseSensitive": false,
                          "matchType": "CONTAINS"
                        },
                        "fieldName": "pagePath"
                      }
                    }
                  },   
                  {
                    "notExpression": {
                      "filter": {
                        "stringFilter": {
                          "value": "(other)",
                          "caseSensitive": false,
                          "matchType": "EXACT"
                        },
                        "fieldName": "pagePath"
                      }
                    }
                  }, 
                  {
                    "notExpression": {
                      "filter": {
                        "stringFilter": {
                          "value": "\/c\/.+\\+",
                          "caseSensitive": false,
                          "matchType": "PARTIAL_REGEXP"
                        },
                        "fieldName": "pagePath"
                      }
                    }
                  },                 
                  {
                    "notExpression": {
                      "filter": {
                        "stringFilter": {
                          "value": "(not set)",
                          "caseSensitive": false,
                          "matchType": "EXACT"
                        },
                        "fieldName": "pagePath"
                      }
                    }
                  }                                                                                                                      
                ]
              }
      };
    }

//Logger.log("2: "+request.dimensionFilter.andGroup.expressions.filter);
const results = AnalyticsData.Properties.runReport(request, 'properties/' + propertyId);

 if (results.rows) {
    Logger.log("        "+results.rows.length+" url's found for ["+contains_a+"] ");
     results_a.push(results);
  }  
  else {
    Logger.log("        no url's found for ["+contains_a+"] ");
  }
}


function fixUrls(){

      Logger.log("fixing urls");
      var sheet = SpreadsheetApp.getActive();
      var ss = sheet.getSheetByName(input);
      var lr = ss.getLastRow();
      var lastrow = lastRow(input,"B",sheet);
      var amount = parseFloat(lr-lastrow-1);
      
      if (amount<1){
        Logger.log("      no urls to fix");
        return;
      }
      else if (amount<2000){
        var urls_a = [];
        for (var i=lastrow;i<lr+1;i++){
          var oldUrl = ss.getRange(i,1).getValue();
           
            if (oldUrl.toString().match("/r/") && !oldUrl.toString().match("/c/")){
              var lastChar = new RegExp(/.{1}$/gim).exec(oldUrl);
              if (lastChar!=="/"){
                var oldUrl = oldUrl+"/";
              }
            }

          var newUrl = fixUrl(oldUrl);
          var newUrl = newUrl.toLowerCase().replace("/products/products/","/products/").replace("//","/");
            urls_a.push([newUrl]);
        }
        var range = ss.getRange(lastrow,2,urls_a.length,1).setValues(urls_a);
      }
      else {
      Logger.log("      "+amount+" urls to fix");
        var times = Math.ceil(parseFloat(amount/1000));

        Logger.log("          "+times+" runs to fix urls");

        for (var y=0;y<times;y++){
            Logger.log("                run ["+y+"] ");
            var lastrow = lastRow(input,"B",sheet);
            var urls_a = [];
            var maxrow = lastRow(input,"A",sheet);
            if (parseFloat(maxrow-lastrow)<1000){
              var x = maxrow;
            }
            else {
              var x = parseFloat(lastrow+1000);
            }
            //Logger.log("                      processing ["+parseFloat(x-lastrow)+"] url's");
            for (var i=lastrow;i<x;i++){
              var oldUrl = ss.getRange(i,1).getValue();
              
                if (oldUrl.toString().match("/r/") && !oldUrl.toString().match("/c/")){
                  var lastChar = new RegExp(/.{1}$/gim).exec(oldUrl);
                  if (lastChar!=="/"){
                    var oldUrl = oldUrl+"/";
                  }
                }

              var newUrl = fixUrl(oldUrl);
              var newUrl = newUrl.toLowerCase().replace("/products/products/","/products/").replace("//","/");
                urls_a.push([newUrl]);
            }
            var range = ss.getRange(lastrow,2,urls_a.length,1).setValues(urls_a);

        }
      }
}

function fixUrls2(){

      Logger.log("fixing urls");
      var sheet = SpreadsheetApp.getActive();
      var ss = sheet.getSheetByName(input);
      var lr = ss.getLastRow();
      var lastrow = lastRow(input,"B",sheet);
      var amount = parseFloat(lr-lastrow-1);
      Logger.log("      "+amount+" urls to fix");

      if (amount<1000){
        var urls_a = [];
        for (var i=lastrow;i<lr+1;i++){
          var oldUrl = ss.getRange(i,1).getValue();
           
            if (oldUrl.toString().match("/r/") && !oldUrl.toString().match("/c/")){
              var lastChar = new RegExp(/.{1}$/gim).exec(oldUrl);
              if (lastChar!=="/"){
                var oldUrl = oldUrl+"/";
              }
            }

          var newUrl = fixUrl(oldUrl);
          var newUrl = newUrl.toLowerCase().replace("/products/products/","/products/").replace("//","/");
            urls_a.push([newUrl]);
        }
        var range = ss.getRange(lastrow,2,urls_a.length,1).setValues(urls_a);
      }
      else {
        var urls_a = [];
        Logger.log("      fixing first 1000 urls");

        for (var i=lastrow;i<lastrow+1000;i++){
          var oldUrl = ss.getRange(i,1).getValue();
           
            if (oldUrl.toString().match("/r/") && !oldUrl.toString().match("/c/")){
              var lastChar = new RegExp(/.{1}$/gim).exec(oldUrl);
              if (lastChar!=="/"){
                var oldUrl = oldUrl+"/";
              }
            }

          var newUrl = fixUrl(oldUrl);
          var newUrl = newUrl.toLowerCase().replace("/products/products/","/products/").replace("//","/");
            urls_a.push([newUrl]);
        }
        var range = ss.getRange(lastrow,2,urls_a.length,1).setValues(urls_a);
        var lastrow_new = lastRow(input,"B",sheet);
        var urls_a = [];
        Logger.log("      fixing second 1000 urls");

        for (var i=lastrow_new;i<lastrow_new+1000;i++){
          var oldUrl = ss.getRange(i,1).getValue();
           
            if (oldUrl.toString().match("/r/") && !oldUrl.toString().match("/c/")){
              var lastChar = new RegExp(/.{1}$/gim).exec(oldUrl);
              if (lastChar!=="/"){
                var oldUrl = oldUrl+"/";
              }
            }

          var newUrl = fixUrl(oldUrl);
          var newUrl = newUrl.toLowerCase().replace("/products/products/","/products/").replace("//","/");
            urls_a.push([newUrl]);
        }
        var range = ss.getRange(lastrow,2,urls_a.length,1).setValues(urls_a);
        var lastrow_new = lastRow(input,"B",sheet);
        var urls_a = [];
        Logger.log("      fixing third 1000 urls");

        for (var i=lastrow_new;i<lastrow_new+1000;i++){
          var oldUrl = ss.getRange(i,1).getValue();
           
            if (oldUrl.toString().match("/r/") && !oldUrl.toString().match("/c/")){
              var lastChar = new RegExp(/.{1}$/gim).exec(oldUrl);
              if (lastChar!=="/"){
                var oldUrl = oldUrl+"/";
              }
            }

          var newUrl = fixUrl(oldUrl);
          var newUrl = newUrl.toLowerCase().replace("/products/products/","/products/").replace("//","/");
            urls_a.push([newUrl]);
        }
        var range = ss.getRange(lastrow,2,urls_a.length,1).setValues(urls_a);        
      }
}

function fixUrlsManually(){

      Logger.log("fixing urls");
      var sheet = SpreadsheetApp.getActive();
      var ss = sheet.getSheetByName(input);
      var lr = ss.getLastRow();
      var lastrow = lastRow(input,"B",sheet);

      if (lr>lastrow-1){
        for (var i=lastrow;i<lr+1;i++){
          var oldUrl = ss.getRange(i,1).getValue();
           
            if (oldUrl.toString().match("/r/") && !oldUrl.toString().match("/c/")){
              var lastChar = new RegExp(/.{1}$/gim).exec(oldUrl);
              if (lastChar!=="/"){
                var oldUrl = oldUrl+"/";
              }
            }

          var newUrl = fixUrl(oldUrl);
          var newUrl = newUrl.toLowerCase().replace("/products/products/","/products/").replace("//","/");
          var newUrl_r = ss.getRange(i,2).setValue(newUrl);
        }
      }
}

function fixUrl(url){

var tasks = determineTasks(url);
         // Logger.log("      "+url+" tasks: "+tasks);

  if (tasks!==0){
      for (var i=0;i<tasks.length;i++){
        var task = tasks[i];
         // Logger.log("      "+url+"          task: "+task);
        var url = generateUrl(url,task); 
        var url = "/products/"+url.toString().split("/products/")[1];
      }
  }
  if (url.toString().match("undefined")){
    return ""; 
  }
  else {
    return url; 
  }
}

function generateUrl(url,task){

  if (task=="CAT-CAT"){
    var old_Values = oldValues;
    var new_Values = newValues;

    for (var i=0;i<old_Values.length;i++){
      var maincat = url.replace(/.*\/products\//,"").toString().split("/")[0];
      var maincatUrl = "/"+maincat+"/"; 
      var old_Value = old_Values[i];
      var new_Value = new_Values[i];
         // Logger.log("      "+url+"          task: "+task+"  [old_Value:"+old_Value+" --> new_Value:"+new_Value+"]");

      var url = url.toString().replace(old_Value,new_Value);

      if (newmaincatValues[i]!==""){
        var url = url.toString().replace(maincatUrl,newmaincatValues[i]);
      }
         // Logger.log("      "+url+"          task: "+task+"  [result:"+url+"]");

    }
    var url = url.toString().replace(/\/\//gim,"/");
    var url = sortFacets(url);
         // Logger.log("      "+url+"          task: "+task+"  [result sorted:"+url+"]");

    return url;
  }

  else if (task=="FACET-FACET"){
    var old_Values = oldFValues;
    var new_Values = newFValues;
    var cat_Urls = catFurls;
    var cat_Urls_new = catFurlsNew;
         // Logger.log("      "+url+"          task: "+task+"  [old_Value:"+old_Value+" --> new_Value:"+new_Value+"]");

    for (var i=0;i<old_Values.length;i++){
      var old_Value = old_Values[i];
      var new_Value = new_Values[i];
      var newCat = catFurlsNew[i];

      if (url.toString().match(old_Value)){
        var url = url.toString().replace(old_Value,new_Value);
      }

      if (newCat!==""){
        var maincatUrl = url.toString().split("/products/")[1].toString().split("/")[0];

        if (!url.toString().match("/r/")){
          var facets = "/c/" + url.toString().split("/c/")[1];
        }
        else {
          var facets = "/r/" + url.toString().split("/r/")[1];
        }
        var url = "/products/"+ maincatUrl + "/" + newCat + facets;
      }
    }

         // Logger.log("      "+url+"          task: "+task+"  [result:"+url+"]");
    var url = url.toString().replace(/\/\//gim,"/");
    var url = sortFacets(url);
         // Logger.log("      "+url+"          task: "+task+"  [result sorted:"+url+"]");

    return url;
  }

  else if (task=="CAT-FACET"){
    var old_Values = old_cat_to_facet;
    var new_Values = new_cat_to_facet;
    var cat_Urls = newer_cat_to_facet;
    var maincat = url.replace(/.*\/products\//,"").toString().split("/")[0];
    var maincatUrl = "/"+maincat;

    for (var i=0;i<old_Values.length;i++){
      var old_Value = old_Values[i];
      var new_Value = new_Values[i];
      var cat_Url = cat_Urls[i];
        //  Logger.log("      "+url+"          task: "+task+"  [old_Value:"+old_Value+" --> new_Value:"+new_Value+"]");

      if (url.toString().match(old_Value)){
          if (url.toString().match("/r/")){
            var facets = "r/"+url.toString().split("/r/")[1];

            if (url.toString().match("/c/")){
              var url = "www.beslist.nl/products"+maincatUrl+cat_Url+facets+"~~"+new_Value;
            }
            else {
              var url = "www.beslist.nl/products"+maincatUrl+cat_Url+facets+"c/"+new_Value;
            }
          }
          else if (url.toString().match("/c/")){
            var facets = "c/"+url.toString().split("/c/")[1];
            var url = "www.beslist.nl/products"+maincatUrl+cat_Url+facets+"~~"+new_Value;
          }
          else {
            var url = "www.beslist.nl/products"+maincatUrl+cat_Url+"/c/"+new_Value;
          }
      }
    }
        // Logger.log("      "+url+"          task: "+task+"  [result:"+url+"]");
    var url = url.toString().replace(/\/\//gim,"/");
    var lastChar = new RegExp(/.{1}$/gim).exec(url);
    
    if (url.toString().match("/r/") && !url.toString().match("/c/") && lastChar!=="/"){
      var url = url+"/";
    }
    var url = sortFacets(url);  

    return url;
  }

  else if (task=="FACET-CAT"){
      Logger.log("    url: "+url)
    var old_Values = old_facet_to_cat;
    var new_Values = new_facet_to_cat;
    var maincat = url.toString().replace(/.*\/products\//,"").split("/")[0];
    var maincatUrl = "/"+maincat+"/";    
      Logger.log("        maincatUrl: "+maincatUrl)

    for (var i=0;i<old_Values.length;i++){
      var old_Value = old_Values[i];
      var new_Value = new_Values[i];

      if (maincatUrl==new_Value || maincatUrl==new_Value.toString().replace(/\//gim,"")){
        var maincatUrl = "";
      }
      Logger.log("           old_Value: "+old_Value)
      Logger.log("           new_Value: "+new_Value)
          //Logger.log("      "+url+" [old_Value:"+old_Value+" --> new_Value:"+new_Value+"]");

      if (url.toString().match(old_Value)){
      Logger.log("                  ["+url+"] matches old value "+old_Value)
        var url = url.toString().replace(old_Value,"");
      Logger.log("                  --->"+url)

        if (url.toString().match(/.*\/c\/$/)){
          var url = url.toString().split("/c/")[0];
          Logger.log("              url has no facets left, /c/ cut off: "+url);
        }
        else if (url.toString().match(/\/c\/~~/)){
          var url = url.toString().replace("/c/~~","/c/");
          Logger.log("              url has no facets left, /c/ cut off: "+url);
        }
        if (url.toString().match("/r/")){
          var facets = "r/"+url.toString().split("/r/")[1];
        }
        else if (url.toString().match("/c/")){
          var facets = "c/"+url.toString().split("/c/")[1];
        }
        else {
          var facets = "";
        }
        var url = "www.beslist.nl/products"+maincatUrl+new_Value+facets;          
      }
    }
         // Logger.log("      "+url+"          task: "+task+"  [result:"+url+"]");
    var url = url.toString().replace(/\/\//gim,"/");
    var lastChar = new RegExp(/.{1}$/gim).exec(url);
    
    if (url.toString().match("/r/") && !url.toString().match("/c/") && lastChar!=="/"){
      var url = url+"/";
    }

    var url = sortFacets(url);

    return url;
  }
}

function determineTasks(url){
//Logger.log("fixing urls");
var tasks_a = [];

//Logger.log("oldValues: "+oldValues);
//Logger.log("oldValues: "+oldFValues);
//Logger.log("oldValues: "+old_cat_to_facet);
//Logger.log("oldValues: "+old_facet_to_cat);

  if (contains(oldValues,url)!==false){
    tasks_a.push("CAT-CAT");
  }
  if(contains(oldFValues,url)!==false){
    tasks_a.push("FACET-FACET");
  }
  if(contains(old_cat_to_facet,url)!==false){
    tasks_a.push("CAT-FACET");
  }
  if(contains(old_facet_to_cat,url)!==false){
    tasks_a.push("FACET-CAT");
  }  

  if (tasks_a.length>0){
    return tasks_a;
  }
  else {
    return 0;
  }
}

function contains(array,value){
    var count=array.length;
    for(var i=0;i<count;i++){
        if(value.match(array[i].toString())){return true;}
    }
    return false;
}

function getOldandNewValues(TYPE){
  var sheet = SpreadsheetApp.getActive();
  var ss = sheet.getSheetByName(replace);
  var lr = ss.getLastRow();

    if (lr==5){
      throw "Geef tenminste één gewijzigde facet- of cat-url op!";
    }

var caturls = [];

  if (TYPE=="CAT-CAT"){
   // //Logger.log("TYPE = CAT-CAT");
  // check cat-url's
      var lr = lastRow(replace,"A",sheet);

      for (i=6;i<lr;i++) {
      //Logger.log("i = "+i);
        var newcaturl = ss.getRange(i,2).getValue().toString().replace(/\//g,"");
            var newcaturl = "/"+newcaturl+"/";

        var oldcaturl = ss.getRange(i,1).getValue().toString().replace(/\//g,"");
            var oldcaturl = "/"+oldcaturl+"/";

        var newmainurl = ss.getRange(i,3).getValue().toString().replace(/\//g,"");
            var newmainurl = "/"+newmainurl+"/";

          if (newcaturl!=="//" && oldcaturl!=="//"){
                newValues.push(newcaturl);
               // caturls.push("");
                oldValues.push(oldcaturl);

                if (newmainurl!=="//"){
                      newmaincatValues.push(newmainurl);
                }
                else {
                      newmaincatValues.push("");
                }
          }
      }
  }

  else if (TYPE=="FACET-FACET"){
      //Logger.log("TYPE = "+TYPE);
  // check facet-url's
      var lr = lastRow(replace,"D",sheet);

      for (i=6;i<lr;i++) {
        var newfaceturl = ss.getRange(i,5).getValue().toString().replace(/\//gim,"");
        var oldfaceturl = ss.getRange(i,4).getValue().toString().replace(/\//gim,"");
        var caturl_old = ss.getRange(i,6).getValue().toString().replace(/\//gim,"");
        var caturl_new = ss.getRange(i,7).getValue().toString().replace(/\//gim,"");

        if (newfaceturl!=="//" && oldfaceturl!=="//" && newfaceturl!=="" && oldfaceturl!==""){
              oldFValues.push(oldfaceturl);
              newFValues.push(newfaceturl);

              if (caturl_old!==""){
                catFurls.push("/"+caturl_old+"/");                
              }
              else {
                catFurls.push("");                
              }              
              if (caturl_new!==""){
                catFurlsNew.push("/"+caturl_new+"/");                
              }
              else {
                catFurlsNew.push("");                
              }              
        }
      }
  }

  else if (TYPE=="CAT-FACET"){
     // Logger.log("TYPE = "+TYPE);
  // check cat-to-facet-url's
      var lr = lastRow(replace,"H",sheet);

      for (i=6;i<lr;i++) {
        var oldcaturl = ss.getRange(i,8).getValue().toString().replace(/\//gim,"");
        var oldcaturl = "/"+oldcaturl+"/";
        var newercaturl = ss.getRange(i,9).getValue();
        var newercaturl = newercaturl.toString().replace(/\//gim,"");
        var newercaturl = "/"+newercaturl+"/";
        var newfaceturl = ss.getRange(i,10).getValue().toString().replace(/\//gim,"");

        if (newfaceturl!=="" && oldcaturl!=="//"){
              old_cat_to_facet.push(oldcaturl);
              new_cat_to_facet.push(newfaceturl);
              newer_cat_to_facet.push(newercaturl);  
        }
      }
  }

  else if (TYPE=="FACET-CAT"){
    //Logger.log("TYPE = FACET-CAT");

  // check facet-to-cat-url's
      var lr = lastRow(replace,"K",sheet);

    //Logger.log(" lr:"+lr);

      for (i=6;i<lr;i++) {
        var newcaturl = ss.getRange(i,12).getValue().replace(/\//gim,"");
            var newcaturl = "/"+newcaturl+"/";
        var oldfaceturl = ss.getRange(i,11).getValue().replace(/\//gim,"");

    //Logger.log("      newcaturl:"+newcaturl);
    //Logger.log("      oldfaceturl:"+oldfaceturl);

        if (newcaturl!=="//" && oldfaceturl!==""){
              old_facet_to_cat.push(oldfaceturl);
              new_facet_to_cat.push(newcaturl);
        }
      }
    //Logger.log("            old_facet_to_cat: "+old_facet_to_cat);
    //Logger.log("            new_facet_to_cat: "+new_facet_to_cat);
  }    
}

function sortFacets(url){
      var R = 0;
      if (url.match("/r/")){
        var R = 1;
        var query = url.toString().split("/r/")[1];

        if (query.toString().match("/c/")){
          var query = query.toString().split("/c/")[0];
        } 
      }

      var facets_a = [];
      var facets = url.toString().split("/c/")[1];
         // Logger.log("                    facets: "+facets);

      if (facets!==undefined){
        if (facets.toString().match("~~")){
            for (var i=0;i<facets.toString().split("~~").length;i++){
              var facet = facets.toString().split("~~")[i];
                facets_a.push(facet.toLowerCase());
            }
        }
      }

      else {
          return url;
      }

      if (facets_a.length>0){
              //Logger.log("                    unsorted facets: "+facets_a);
          var facets = facets_a.sort();
              //Logger.log("                    sorted facets: "+facets_a);

          var newUrl = url.toString().split("/c/")[0]+"/c/"+facets_a.join("~~");
      }
      else {
          var newUrl = url;
      }

      var newUrl = newUrl.replace("/c/~~","/c/");
          //Logger.log("   NewUrl: "+newUrl);
          return newUrl;
}

function checkStatuscodes() {
  var statuscodes_a = [];

  var sheet = SpreadsheetApp.getActive();
  var ss = sheet.getSheetByName(input);
  var lr = ss.getLastRow();
  var lastR = lastRow(input,"C",sheet);

    for (var i=lastR;i<lr+1;i++){
        var statusspace = ss.getRange(i,3);
        var url = "https://www.beslist.nl"+ss.getRange(i,2).getValue();

        try{
          var urltest1 = testURL(url).toString();
        }
        catch(err){
            Logger.log("          [ERROR] url: "+url);
            var urltest1 = "-"
        }
        finally{
          statuscodes_a.push([urltest1]);
          //statusspace.setValue(urltest1);
        }
    }
    var range = ss.getRange(lastR,3,statuscodes_a.length,1).setValues(statuscodes_a);
} 

function checkStatuscodesManually() {
  var sheet = SpreadsheetApp.getActive();
  var ss = sheet.getSheetByName(input);
  var lr = ss.getLastRow();
  var lastR = lastRow(input,"C",sheet);

    for (var i=lastR;i<lr+1;i++){
        var statusspace = ss.getRange(i,3);
        var url = "https://www.beslist.nl"+ss.getRange(i,2).getValue();

        try{
          var urltest1 = testURL(url).toString();
        }
        catch(err){
            Logger.log("          [ERROR] url: "+url);
            var urltest1 = "-"
        }
        finally{
          statusspace.setValue(urltest1);
        }
    }
} 

function SimplygenerateURLs() {

  //getOldandNewValues();
             // //Logger.log("OLD "+oldValues);
             // //Logger.log("NEW "+newValues);
  
  var sheet = SpreadsheetApp.getActive();
  var ss = sheet.getSheetByName(input);
  var lr = ss.getLastRow();
  var checkResult = ss.getRange(2,3).getValue();

  if (checkResult!=="JA" && checkResult!=="NEE"){
      throw "Geef aan of gegenereerde url's moeten worden getest!";
  }

    for (var i=5;i<lr;i++){

      var oldurlspace = ss.getRange(i,1);
      var oldurl = oldurlspace.getValue();

        if (!oldurl.match("beslist.nl")){
          var oldurl = "https://www.beslist.nl/products/"+oldurl.toString().split("/products/")[1];
      //    //Logger.log("oldurl had no beslist.nl ["+oldurl+"]");
        }

      var newurlspace = ss.getRange(i,2);
      var statusspace = ss.getRange(i,3);
      
      if (oldurl!==""){
     //   //Logger.log("row "+i+" start url: "+oldurl);
          var newUrl = getNewUrl(oldurl);
        //  //Logger.log("newUrl "+i+": "+newUrl);

          if (checkResult=="JA"){

              var urltest1 = testURL(newUrl).toString();

              //  //Logger.log("urltest1 = "+urltest1);

                if (urltest1.match("beslist")){
                  newurlspace.setValue(urltest1.toString().split("beslist.nl")[1]);
                  statusspace.setValue("200");

                  if (oldurl.match("beslist.nl")){
                        var oldurlnew = oldurl.toString().split("beslist.nl")[1];
                        oldurlspace.setValue(oldurlnew);
                  }
                }
                else if (urltest1=="200"){
                  newurlspace.setValue(newUrl.toString().split("beslist.nl")[1]);
                  statusspace.setValue("200");

                  if (oldurl.match("beslist.nl")){
                        var oldurlnew = oldurl.toString().split("beslist.nl")[1];
                        oldurlspace.setValue(oldurlnew);
                  }
                }
                else if (urltest1=="404"){
                  newurlspace.setValue(newUrl.toString().split("beslist.nl")[1]);
                  statusspace.setValue("404");
                
                }
                else {
                  newurlspace.setValue(newUrl.toString().split("beslist.nl")[1]);
                  statusspace.setValue(urltest1);

                  if (oldurl.match("beslist.nl")){
                        var oldurlnew = oldurl.toString().split("beslist.nl")[1];
                        oldurlspace.setValue(oldurlnew);
                  }              
                }
          }
          else if (checkResult=="NEE"){

                  newurlspace.setValue(newUrl.toString().split("beslist.nl")[1]);
          
          }

          //  //Logger.log("    row "+i+" final url: "+newurlspace.getValue());
      }
    }
}

function testURL(url){  
 // //Logger.log(url);

  var response = UrlFetchApp.fetch(url, {'followRedirects': false, 'muteHttpExceptions': true});
  var responseCode = response.getResponseCode();

    return responseCode;
}

function getNewUrl(oldurl){

    for (var i=0;i<oldValues.length;i++){
      var oldValue = oldValues[i];
      var newValue = newValues[i];
      var oldurl = oldurl.toString().replace(oldValue,newValue);

  return oldurl;
    }
}

function lastRow(sheet,column,ss) {
  if (column == null) {
    if (sheet != null) {
       var sheet = ss.getSheetByName(sheet);
    } else {
      var sheet = ss.getActiveSheet();
    }
    return sheet.getLastRow();
  } else {
    var sheet = ss.getSheetByName(sheet);
    var lastRow = sheet.getLastRow();
    var array = sheet.getRange(column + 1 + ':' + column + lastRow).getValues();
    for (i=0;i<array.length;i++) {
      if (array[i] != '') {       
        var final = i + 2;
      }
    }
    if (final != null) {
      return final;
    } else {
      return 0;
    }
  }
}

function outputToSpreadsheet_a(){
Logger.log("    [outputting array of results]");
  for (y=0;y<results_a.length;y++) {
    var ss = sheet.getSheetByName(input);
    const rows = results_a[y].rows.map((row) => {
      const dimensionValues = row.dimensionValues.map(
          (dimensionValue) => {
            return dimensionValue.value;
          });

      return [...dimensionValues];
    });
    var lr = ss.getLastRow();
    //ss.getRange(lr+1, 1, report.rows.length, headers.length)
    ss.getRange(lr+1, 1, results_a[y].rows.length)
        .setValues(rows);
  }
}

function getLastNdays(nDaysAgo) {
  var today = new Date();
  var before = new Date();
  before.setDate(today.getDate() - nDaysAgo);
  return Utilities.formatDate(before, 'GMT', 'yyyy-MM-dd');
}

function check(array,value){
    var count=array.length;
    for(var i=0;i<count;i++){
        if(array[i]==value){return true;}
    }
    return false;
}