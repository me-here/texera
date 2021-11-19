import { GooglePeopleApiResponse } from "./../../type/google-api-response";
import { Component, OnInit, Input } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { HttpClient } from "@angular/common/http";
import { environment } from "../../../../environments/environment";

@UntilDestroy()
@Component({
  selector: "texera-user-avatar",
  templateUrl: "./user-avatar.component.html",
  styleUrls: ["./user-avatar.component.scss"],
})

/**
 * UserAvatarComponent is used to show the avatar of a user
 * The avatar of a Google user will be its Google profile picture
 * The avatar of a normal user will be a default one with the initial
 *
 * @author Zhen Guan
 */
export class UserAvatarComponent implements OnInit {
  public googleUserAvatarSrc: string = "";
  private publicKey = environment.google.publicKey;

  constructor(private http: HttpClient) {}

  @Input() googleId?: string;
  @Input() userName?: string;

  ngOnInit(): void {
    if (!this.googleId && !this.userName) {
      throw new Error("google Id or user name should be provided");
    }
    if (this.googleId) {
      this.userName = "";
      // get the avatar of the google user
      const url = `https://people.googleapis.com/v1/people/${this.googleId}?personFields=names%2Cphotos&key=${this.publicKey}`;
      this.http
        .get<GooglePeopleApiResponse>(url)
        .pipe(untilDestroyed(this))
        .subscribe(res => {
          this.googleUserAvatarSrc = res.photos[0].url;
        });
    } else if (this.userName) {
      const colors = ["#9ACD32", "#40E0D0", "#696969", "#9932CC", "#FF8C00"];
      const avatar = document.getElementById("texera-user-avatar");
      const randomColor = Math.floor(Math.random() * colors.length);
      if (avatar) {
        avatar.style.backgroundColor = colors[randomColor];
      }
    }
  }

  public getUserInitial(userName: string): string {
    return userName
      .split(" ")
      .map(n => n[0])
      .join("");
  }
}
