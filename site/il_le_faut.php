<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<link rel="stylesheet" type="text/css" href="css/geocss.css">
<title>Geo Twitter</title>
</head>

<body>
<div id="head"><img class="twit" src="images/images.gif" /><img class="headimg"  src="images/??????????.png" /></div>
<div id="bodydiv">
<div id="leftdiv">
<div id="searchbox">
<div id="searchbar">
<p class="text">Type Twitter account id and Click Geo</p>

<form name="registration" method="post" action="il_le_faut.php" enctype="multipart/form-data">
<input type="text" name="user_id" class="textin"  />
<input type="submit" name="Valider" value="Rechercher">

</form>
<form class="mapchb">
<input type="checkbox" class="checkbox" name="map" value="googlemap"  />See on map 
<form/>
</div >
</div>
<a href="#" class="Geo">Geo</a>
<body>
	<div id="EmplacementDeMaCarte">
	
	</div>
		
	</body>
<div id="rightdiv"></div>

</div>
<div id="tail"></div>
</body>
</html>

</html>
<?php
//On remplie la base de donn�es seulement pour effectuer des tests...
mysql_connect("localhost", "root", "");     
mysql_select_db("gtdb");
mysql_query ("INSERT INTO tw_user VALUES('1','Yassine_chaab','yassine chaab','urlnameyassine')");
mysql_query ("INSERT INTO tw_user VALUES('2','Wilmer_Calle_1','wilmer calle','urlnamewilmer')");
mysql_query ("INSERT INTO tw_tweet VALUES('1','tweet_user1_Annaba',CURRENT_TIMESTAMP,'36.916733','7.767253','boulevard Saint-cloud,Annaba, Algerie','1' )");
mysql_query ("INSERT INTO tw_tweet VALUES('2','tweet_user1_Saint-Petersbourg',CURRENT_TIMESTAMP,'59.945641','30.335283','Saint-Petersbourg, Russie','1' )");
mysql_query ("INSERT INTO tw_tweet VALUES('3','tweet_user1_Sao-Paolo',CURRENT_TIMESTAMP,'-23.526218','-46.635819','Sao Paulo, bresil','1' )");
mysql_query ("INSERT INTO tw_tweet VALUES('4','tweet_user2_Saint-Petersbourg',CURRENT_TIMESTAMP,'59.945641','30.335283','Saint-Petersbourg, Russie','2' )");


?>
<?php session_start(); 

if (isset($_POST['user_id'])) {if ($_POST['user_id']!=null) {  
         //mysql_connect("localhost", "root", "");     
         //mysql_select_db("gtdb");
         $user_id = $_POST['user_id'];
         $reponse = mysql_query("SELECT user_id FROM tw_user");
         $trouve2 = false ;
         while ($donnees = mysql_fetch_array($reponse)  AND $trouve2 == false)
       { 
       if  ($user_id == $donnees['user_id']) { $trouve2 = true; }
      }
//mysql_close();




//On affiche le contenu des tables sur le site...

$reponse=mysql_query("SELECT screen_name,urlname FROM tw_user WHERE user_id='$user_id'");
$reponse=mysql_query("SELECT * FROM tw_user,tw_tweet WHERE tw_user.user_id=tw_tweet.user_id AND tw_user.user_id= $user_id");
$donnees = mysql_fetch_array($reponse);
$screen_name = $donnees['screen_name'];
//$urlname = $donnees['urlname'];
$tweet_text = $donnees['tweet_text'];
$tweet_date = $donnees['tweet_date'];
$latitude = $donnees['latitude'];
$longitude = $donnees['longitude'];
$place_full_name = $donnees['place_full_name'];

/*?>
<table width="600" border="1" align="center" cellpadding="5" cellspacing="0" bgcolor="#EEEEFF" valign="5">

<tr>
<td width="200" align="center" class="num"><B> User id </B></td>
<td width="200" align="center" class="num"><B> Realname </B></td>
<td width="200" align="center" class="num"><B> Coordinates.Latitude </B></td>
<td width="200" align="center" class="num"><B> Coordinates.Longitude </B></td>
<td width="200" align="center" class="num"><B> Time </B></td>
<?php echo '<tr>'.'<td width="200" align="center">'.$user_id.'</td>'.'<td width="200" align="center">'.$screen_name.'</td>'.'<td width="200" align="center">'.$latitude.'</td>'.'<td width="200" align="center">'.$longitude.'</td>'.'<td width="200" align="center">'.$tweet_date.'</td>'.'</tr>'; 
*/}}

if (isset($_POST['user_id'])) {if ($_POST['user_id']!=null) {

?>
<div id="emptab">
<table width="600" border="1" align="center" cellpadding="5" cellspacing="0" bgcolor="#EEEEFF" valign="5">

<tr>
<td width="200" align="center" class="num"><B> User id </B></td>
<td width="200" align="center" class="num"><B> Realname </B></td>
<?php echo '<tr>'.'<td width="200" align="center">'.$user_id.'</td>'.'<td width="200" align="center">'.$screen_name.'</td>'.'</tr>'; 


	
	

/*
$reponse=mysql_query("SELECT latitude, longitude FROM tw_user,tw_tweet WHERE tw_user.user_id=tw_tweet.user_id AND tw_user.user_id= $user_id");
 while ($donnee = mysql_fetch_array($reponse)){

 ?>
<tr> <td width="200" align="center" class="num"><B> Coordinates </B></td></tr>
<?php echo '<tr>'.'<td width="200" align="center">'.$donnee['latitude'].'--'.$donnee['longitude'].'</td>'.'</tr>';
*/}}
?></div></table>
<td width = 150 valign="top" style="text-decoration: underline; color: #4444ff;">
           <div id="side_bar"  style="overflow:auto; height:450px;">
		   
		   </div>
        </td>

	







<head>
  <meta name="viewport" content="initial-scale=1.0, user-scalable=no">
    <meta charset="utf-8">
    <style>
#EmplacementDeMaCarte {
        height: 550px;
		width: 1350px;
		margin-top:15px;
		margin-left:55px;
        padding: 1px;
      }
#emptab {
	width:600px;
	height:150px;
	margin-left:420px;
	background-color:#00CCFF;
	}
    </style>
	
	
	<SCRIPT LANGUAGE="JavaScript">
var latitude = '<?php echo $latitude; ?>' ; 
var longitude = '<?php echo $longitude; ?>' ; 
</SCRIPT> 
<html lang="fr">
	<head>
		<meta name="viewport" content="initial-scale=1.0, user-scalable=no"/>
		<meta charset="UTF-8" />
		<title>Geotwitter</title>
		<style type="text/css">
			html {
				height: 100%
			}
			body {
				height: 100%;
				margin: 0;
				padding: 0
			}
			
		</style>
		 
			 <script type="text/javascript"
      src="https://maps.googleapis.com/maps/api/js?key=AIzaSyD1fcIqlgjxrMFtt8X7xxRvS4ugzF0c9OU&sensor=false">
    </script>
		<script type="text/javascript">
			function initialisation(){
			
			var map = new google.maps.Map(document.getElementById('EmplacementDeMaCarte'), {
      zoom: 4,
      center: new google.maps.LatLng(48.862004,2.352047),
      mapTypeId: google.maps.MapTypeId.ROADMAP
    });
var bounds = new google.maps.LatLngBounds();
var gmarkers = [];
var side_bar_html = "";	
var i=0;
//var tableau = new Array();


 



<?php 
$reponse=mysql_query("SELECT tweet_text, tweet_date, latitude, longitude, place_full_name FROM tw_user,tw_tweet WHERE tw_user.user_id=tw_tweet.user_id AND tw_user.user_id= $user_id ORDER BY tweet_id " );
 while ($donnee = mysql_fetch_array($reponse)){ 

 
 ?>



<?php echo $latitude; ?>

 
 
 
  /*  var marker;
	var contenu = '<?php echo $donnee['place_full_name']; ?>';
  

	
	marker = new google.maps.Marker({
        position: new google.maps.LatLng('<?php echo $donnee['latitude']; ?>', '<?php echo $donnee['longitude']; ?>'),
        map: map,
		title: '<?php echo $donnee['place_full_name']; ?>'
		});
     
		name = '<?php echo $donnee['place_full_name']; ?>';
		
		gmarkers[i] = marker;
    
		side_bar_html += '<b onclick="javascript:myclick(' + i + ')">' + name + '<\/b><br>';*/
//var infowindow = new google.maps.InfoWindow();
tweet_text = '<?php echo $donnee['tweet_text']; ?>';
tweet_date = '<?php echo $donnee['tweet_date']; ?>';

var point = new google.maps.LatLng('<?php echo $donnee['latitude']; ?>','<?php echo $donnee['longitude']; ?>');
name = '<?php echo $donnee['place_full_name']; ?>';
		var marker = createMarker(i);
		var infowindow = new google.maps.InfoWindow();
		
//		google.maps.event.addListener(marker,'click',function(){
//        infowindow.setContent(this.title);
//        infowindow.open(map,marker);
	
		
//         });
	

	//	bounds.extend(point);
	//	map.fitBounds(bounds);

//};		

	//document.write('<p>Le tableau comprend ' + gmarkers.length + ' valeurs.</p>');
	//document.write('<p>valeur ' + gmarkers[i] + ' </p>');
	i = i + 1;  
<?php } ?>			 



this.myclick=function(i) {
       google.maps.event.trigger(gmarkers[i], 'click');
    };	

function createMarker(){

        var marker = new google.maps.Marker({
            position: point,  
            map: map,
            title: name
        });
side_bar_html += '<tr><td width="200" align="center">--' + tweet_text + '--</td><td width="200" align="center">--' + tweet_date + '--</td><td width="200" align="center"><b onclick="javascript:myclick(' + i + ')">--' + name + '--<\/b><br></td></tr>';
        
		var infowindow = new google.maps.InfoWindow();
		google.maps.event.addListener(marker,'click',function(){
      //  infowindows.setCenter(point);
		map.panTo(this.getPosition());
		infowindow.setContent(this.title);
        infowindow.open(map,marker);
        }); 

            //google.maps.event.addListener(marker,'click',function(){
            //window.location.href = marker.url;
            //});   

        gmarkers[i] = marker;

    };	


//function myclick(i) {
//      GEvent.trigger(gmarkers[i],"click");}		
//function myclick(i) {
//       google.maps.event.trigger(gmarkers[i],"click");};	
 

		
		
			
		document.getElementById("side_bar").innerHTML = side_bar_html;    
			
			
			
			}
 
  
  
  
	  
	  //}
		
			 google.maps.event.addDomListener(window, 'load', initialisation);
		</script>
	</head>
	
	
	