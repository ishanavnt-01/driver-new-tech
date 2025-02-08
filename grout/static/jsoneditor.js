function JSONEditor(container){
  this.container=container;
  this.set=function(parameter){
    if(!parameter)
      return;
    if(!parameter.properties)
      return;
    for(var k in parameter.properties){
      this.container.innerHTML=this.container.innerHTML+'<ul><a class="subtypes">'+k+'</a>';
      var definition=parameter.schema.definitions[k];
      this.container.innerHTML=this.container.innerHTML+'<li>Descrption <a class="subtypes_fields">'+definition['description']+'</a></li>';
      this.container.innerHTML=this.container.innerHTML+'<li>Title <a class="subtypes_fields">'+definition['title']+'</a></li>';
      this.container.innerHTML=this.container.innerHTML+'<li>Plural title <a class="subtypes_fields">'+definition['plural_title']+'</a></li>';
      this.container.innerHTML=this.container.innerHTML+'</ul>';
      for(var k in parameter.schema.definitions[k]){
        this.container.innerHTML=this.container.innerHTML+'<a class="subtype_pro">'+k+'</a>';
      }
    }
  }
  return this;
}