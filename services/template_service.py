from models import db, EtlTemplate

class TemplateService:
    def get_all_templates(self):
        return EtlTemplate.query.all()

    def save_template(self, name, type, content):
        new_template = EtlTemplate(
            template_name=name,
            template_type=type,
            template_content=content
        )
        db.session.add(new_template)
        db.session.commit()
        return new_template

    def get_template(self, id):
        return EtlTemplate.query.get(id)
